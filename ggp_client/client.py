"""Async access to the GGP portal: sync the catalogue, log in, download and unpack files."""
import asyncio
import getpass
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from email.message import Message
from pathlib import Path

import httpx

from . import db, extract, ffs_conversion, parse

logging.getLogger("httpx").setLevel(logging.WARNING)  # its INFO lines print request URLs: the access link is one
log = logging.getLogger("ggp_client")  # never log access_url: it works as a password
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"


class LoginError(RuntimeError):
    pass


@dataclass
class ggpClient:
    """The catalogue (self.con), the rows your filters select (self.rows), and the portal session.

    project, round, wave, country, ids and all_versions are the filters of db.select;
    None means all. self.rows is refreshed after sync and download.
    force: download and overwrite even files that already exist; by default existing files are never touched.
    Sync and downloads are logged to the "ggp_client" logger and to data_dir/ggp_client.log.
    """
    data_dir: str | Path = "ggp_data"
    access_url: str | None = field(default=None, repr=False)  # secret: never logged, stored or put in errors
    concurrency: int = 8
    project: object = None
    round: object = None
    wave: object = None
    country: object = None
    ids: list | None = None
    all_versions: bool = False
    force: bool = False
    transport: object = field(default=None, repr=False)

    def __post_init__(self):
        self.dir = Path(self.data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.access_url = self.access_url or os.environ.get("GGP_ACCESS_URL")
        if self.concurrency < 1:
            raise ValueError("concurrency must be 1 or more")  # 0 would wait forever
        self.con = db.connect(self.dir / "catalogue.sqlite")
        self.rows = self.select()  # raises ValueError for a filter value that is not in the catalogue
        self.sem = asyncio.Semaphore(self.concurrency)
        self.client = httpx.AsyncClient(
            base_url=parse.BASE, follow_redirects=True, timeout=60,
            transport=self.transport, headers={"User-Agent": "ggp-client/0.1"},
        )
        self._log_file = logging.FileHandler(self.dir / "ggp_client.log", encoding="utf-8")
        self._log_file.setFormatter(logging.Formatter(LOG_FORMAT))
        log.addHandler(self._log_file)
        if log.level == logging.NOTSET:  # INFO by default; a level you set yourself is kept
            log.setLevel(logging.INFO)

    def select(self):
        """The downloadable rows that match this client's filters."""
        return db.select(self.con, self.project, self.round, self.wave, self.country, self.ids,
                         self.all_versions, downloadable=True)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.client.aclose()
        log.removeHandler(self._log_file)
        self._log_file.close()

    async def _text(self, url):
        for attempt in range(3):
            try:
                async with self.sem:
                    r = await self.client.get(url)
                if r.status_code < 500:
                    r.raise_for_status()
                    return r.text
            except httpx.TransportError as e:
                log.warning("%s: %s, attempt %d of 3", url, type(e).__name__, attempt + 1)
            else:
                log.warning("%s: HTTP %d, attempt %d of 3", url, r.status_code, attempt + 1)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"giving up on {url}")

    async def sync(self, dump=None):
        """Index the catalogue. Returns (found, failed) by slug.

        The public pages need no login. Harmonized Histories and Additional Datasets are listed only
        on the data access page, so sync logs in (see login) to add them; without a login they are
        skipped and reported in failed, and the public part is still indexed.
        """
        slugs = parse.parse_catalog(await self._text(parse.CATALOG_URL))
        seen, found, failed, todo = set(slugs), {}, {}, slugs
        if dump:
            Path(dump).mkdir(parents=True, exist_ok=True)
        while todo:  # each round also visits "other versions" discovered on the previous one
            pages = await asyncio.gather(*(self._text(parse.page_url(s)) for s in todo),
                                         return_exceptions=True)
            nxt = []
            for slug, html in zip(todo, pages):
                if isinstance(html, Exception):
                    failed[slug] = str(html) or type(html).__name__
                    log.error("sync: %s failed: %s", slug, failed[slug])
                    continue
                if dump:
                    (Path(dump) / f"{slug}.html").write_text(html, encoding="utf-8")
                found[slug] = parse.parse_dataset(html, slug)
                nxt += [s for s in found[slug]["other_versions"] if s not in seen]
                seen.update(found[slug]["other_versions"])
            todo = nxt
        try:
            await self.login()
            html = await self._text("/form/")
            if dump:
                (Path(dump) / "form.html").write_text(html, encoding="utf-8")
            found.update(parse.parse_access(html))
        except (RuntimeError, httpx.HTTPError) as e:  # LoginError is a RuntimeError
            failed["data access page"] = f"Harmonized Histories and Additional Datasets skipped: {e}"
            log.warning("sync: %s", failed["data access page"])
        for slug in list(found):  # the latest version often has its own -vX-Y page too: keep only the latest row
            base = re.sub(r"-v\d+-\d+$", "", slug)
            if base != slug and base in found and found[base]["version"] == parse.parse_slug(slug)["version"]:
                del found[slug]
        for slug in sorted(found, key=parse.sort_key):  # a fresh db numbers datasets in this order
            db.upsert(self.con, slug, found[slug])
        self.con.commit()
        self.rows = self.select()
        log.info("sync: indexed %d datasets, %d failed", len(found), len(failed))
        return found, failed

    async def _logged_in(self):
        return "/logout/" in await self._text("/form/")  # only a logged-in page has the logout form

    async def login(self):
        """Reuse the saved session if it still works, else log in with the access link.

        The link comes from the access_url argument, then the GGP_ACCESS_URL environment variable,
        and if neither is set you are asked for it (input hidden, it works as a password).
        """
        jar = self.dir / ".session.json"
        try:
            saved = json.loads(jar.read_text())
        except (OSError, ValueError):  # no file, or a half-written one: log in again
            saved = []
        for c in saved:
            self.client.cookies.set(c["name"], c["value"], domain=c["domain"], path=c["path"])
        if saved and await self._logged_in():
            log.info("login: saved session reused")
            return
        url = (self.access_url or "").strip()
        if not url:
            try:
                url = (await asyncio.to_thread(
                    getpass.getpass, "No valid session. Paste the emailed access link (input is hidden): ")).strip()
            except (EOFError, OSError):  # no terminal to ask on
                pass
        if not url:
            raise LoginError("no valid session: pass access_url, set GGP_ACCESS_URL, or run in a terminal to be "
                             "asked (request a fresh link by email on the portal login page)")
        if not url.startswith(parse.BASE + "/"):
            raise LoginError(f"the access link must start with {parse.BASE}/")
        try:
            await self.client.get(url)
        except httpx.HTTPError as e:
            raise LoginError(f"login request failed ({type(e).__name__})") from None
        if not await self._logged_in():
            raise LoginError("the access link was not accepted (expired or already used?)")
        cookies = [dict(name=c.name, value=c.value, domain=c.domain, path=c.path)
                   for c in self.client.cookies.jar]
        tmp = jar.with_suffix(".tmp")  # write then rename: a crash never leaves a broken session file
        tmp.unlink(missing_ok=True)  # O_EXCL below then always creates it with mode 600
        with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
            json.dump(cookies, f)
        os.replace(tmp, jar)
        log.info("login: logged in with the access link, session saved")

    async def download(self, rows=None, root=None, ext=None, metadata=False):
        """Download the files of `rows` (default: self.rows) and unpack them under root (default: data_dir/downloads).

        ext: datafile type(s) to keep (dta, sav, csv): one value, a list, or None for all three.
        metadata: also keep the non-data files (documentation, codebooks) from the archives.
        A file that was already unpacked for these options is skipped; asking for more later
        (another ext, or metadata) downloads it again and adds the missing files to the folder.
        Files that already exist are never overwritten, unless the client was made with force=True.
        FFS: the .dat, .sps and .tbl files are always kept, as ffs/<cc>/<stem>.dat etc.; with ext given the data
        is also converted (ffs_conversion) to ffs/<cc>/converted_data.<ext>, with variable and value labels.
        Returns (done, skipped, [(slug, error)]).
        """
        rows = self.rows if rows is None else rows
        root = Path(root or self.dir / "downloads")
        want = extract.norm_ext(ext)
        convert = sorted(want) if parse.many(ext) else []  # FFS converts only when ext was asked for
        meta = {"meta"} if metadata else set()
        kept = want | meta
        done = skipped = 0
        failed = []

        async def one(r):
            nonlocal done, skipped
            k = ({"raw"} | set(convert) | meta) if r["project"] == "FFS" else kept
            there = bool(r["local_path"]) and (root / r["local_path"]).exists()
            prev = set((r["fetched"] or "").split()) if there else set()  # a deleted folder holds nothing
            if not self.force and there and k <= prev:
                skipped += 1
                log.info("skipped %s: already in %s", r["slug"], r["local_path"])
                return
            log.info("downloading %s (file %d)", r["slug"], r["download_id"])
            try:
                rel, n, size = await self._fetch(r, root, want, metadata, convert)
            except Exception as e:  # one bad file must not stop the batch
                failed.append((r["slug"], str(e) or type(e).__name__))
                log.error("failed %s: %s", r["slug"], failed[-1][1])
                return
            db.mark_downloaded(self.con, r["download_id"], rel, k | (prev if rel == r["local_path"] else set()))
            done += 1
            log.info("downloaded %s: %.1f MB, %d files kept in %s", r["slug"], size / 1e6, n, rel)

        log.info("download: %d files, keeping %s", len(rows), " ".join(sorted(kept)))
        await asyncio.gather(*(one(r) for r in rows))
        log.info("download: %d done, %d skipped, %d failed", done, skipped, len(failed))
        self.rows = self.select()  # local_path and fetched changed
        return done, skipped, failed

    async def _fetch(self, r, root, want, metadata, convert=()):
        stem = r["stem"] + (f"_{r['download_id']}" if r["n_files"] > 1 else "")
        ffs = r["project"] == "FFS"
        rel = r["rel_dir"] if ffs else f"{r['rel_dir']}/{stem}"  # FFS: one dataset per country, no folder of its own
        dest = root / rel
        existed = dest.exists()  # a failed fetch must not delete what an earlier one unpacked
        part = dest.with_name(stem + ".part")
        ok, size = False, 0
        try:
            async with self.sem, self.client.stream("GET", r["url"]) as resp:
                resp.raise_for_status()
                if "text/html" in resp.headers.get("content-type", ""):
                    raise RuntimeError("got a web page instead of a file (no access to this dataset yet?)")
                disposition = Message()  # parses filename*=UTF-8''... and quoting, as mail headers do
                disposition["content-disposition"] = resp.headers.get("content-disposition", "")
                name = disposition.get_filename() or stem
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(part, "wb") as f:  # shortcut: blocking write per 64 KB chunk, use aiofiles if it ever matters
                    async for chunk in resp.aiter_bytes(1 << 16):
                        f.write(chunk)
                        size += len(chunk)
            if ffs:  # a conversion error fails this file, not the batch
                n = await asyncio.to_thread(ffs_conversion.unpack, part, name, dest, stem, want, metadata,
                                            convert, self.force)
            else:
                n = await asyncio.to_thread(extract.unpack_file, part, name, dest, want, metadata, self.force)
            ok = n > 0
        finally:
            part.unlink(missing_ok=True)
            if not (ok or existed):
                shutil.rmtree(dest, ignore_errors=True)
        if not ok:
            raise RuntimeError(f"no matching files in the archive (looked for {', '.join(sorted(want))})")
        return rel, n, size

"""Async access to the GGP portal: sync the catalogue, log in, download and unpack files."""
import asyncio
import getpass
import json
import os
import re
import shutil
from pathlib import Path

import httpx

from . import db, extract, parse

DISPOSITION = re.compile(r'''filename\*?=(?:UTF-8'')?"?([^";]+)''', re.I)


class LoginError(RuntimeError):
    pass


class ggpClient:
    def __init__(self, data_dir, access_url=None, concurrency=8, transport=None):
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        # secret: never logged, stored or put in error messages
        self.access_url = access_url or os.environ.get("GGP_ACCESS_URL")
        self.sem = asyncio.Semaphore(concurrency)
        self.client = httpx.AsyncClient(
            base_url=parse.BASE, follow_redirects=True, timeout=60,
            transport=transport, headers={"User-Agent": "ggp-client/0.1"},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.client.aclose()

    async def _text(self, url):
        for attempt in range(3):
            try:
                async with self.sem:
                    r = await self.client.get(url)
                if r.status_code < 500:
                    r.raise_for_status()
                    return r.text
            except httpx.TransportError:
                pass
            await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"giving up on {url}")

    async def sync(self, con, dump=None):
        """Index the public catalogue (no login needed). Returns (found, failed) by slug."""
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
                    continue
                if dump:
                    (Path(dump) / f"{slug}.html").write_text(html, encoding="utf-8")
                found[slug] = parse.parse_dataset(html, slug)
                nxt += [s for s in found[slug]["other_versions"] if s not in seen]
                seen.update(found[slug]["other_versions"])
            todo = nxt
        for slug in sorted(found, key=parse.sort_key):  # a fresh db numbers datasets in this order
            db.upsert(con, slug, found[slug])
        con.commit()
        return found, failed

    async def _logged_in(self):
        return "secure login link" not in await self._text("/form/")

    async def login(self):
        """Reuse the saved session if it still works, else log in with the access link.

        The link comes from the access_url argument, then the GGP_ACCESS_URL environment variable,
        and if neither is set you are asked for it (input hidden, it works as a password).
        """
        jar = self.dir / ".session.json"
        if jar.exists():
            for c in json.loads(jar.read_text()):
                self.client.cookies.set(c["name"], c["value"], domain=c["domain"], path=c["path"])
            if await self._logged_in():
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
        with os.fdopen(os.open(jar, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
            json.dump(cookies, f)

    async def download(self, con, rows, root, force=False, ext=None, metadata=False):
        """Download the files of `rows` and unpack them under root.

        ext: datafile type(s) to keep (dta, sav, csv): one value, a list, or None for all three.
        metadata: also keep the non-data files (documentation, codebooks) from the archives.
        A file that was already unpacked for these options is skipped; asking for more later
        (another ext, or metadata) downloads it again and adds to the folder.
        Returns (done, skipped, [(slug, error)]).
        """
        root = Path(root)
        want = extract.norm_ext(ext)
        kept = want | ({"meta"} if metadata else set())
        done = skipped = 0
        failed = []

        async def one(r):
            nonlocal done, skipped
            if (not force and r["local_path"] and (root / r["local_path"]).exists()
                    and kept <= set((r["fetched"] or "").split())):
                skipped += 1
                return
            try:
                rel = await self._fetch(r, root, want, metadata)
            except Exception as e:  # one bad file must not stop the batch
                failed.append((r["slug"], str(e) or type(e).__name__))
                return
            db.mark_downloaded(con, r["download_id"], rel, kept)
            done += 1

        await asyncio.gather(*(one(r) for r in rows))
        return done, skipped, failed

    async def _fetch(self, r, root, want, metadata):
        stem = r["stem"] + (f"_{r['download_id']}" if r["n_files"] > 1 else "")
        rel = f"{r['rel_dir']}/{stem}"
        dest = root / rel
        existed = dest.exists()  # a failed fetch must not delete what an earlier one unpacked
        part = dest.with_name(stem + ".part")
        ok = False
        try:
            async with self.sem, self.client.stream("GET", r["url"]) as resp:
                resp.raise_for_status()
                if "text/html" in resp.headers.get("content-type", ""):
                    raise RuntimeError("got a web page instead of a file (no access to this dataset yet?)")
                m = DISPOSITION.search(resp.headers.get("content-disposition", ""))
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(part, "wb") as f:  # shortcut: blocking write per 64 KB chunk, use aiofiles if it ever matters
                    async for chunk in resp.aiter_bytes(1 << 16):
                        f.write(chunk)
            ok = await asyncio.to_thread(extract.unpack_file, part, m[1] if m else stem, dest, want, metadata) > 0
        finally:
            part.unlink(missing_ok=True)
            if not (ok or existed):
                shutil.rmtree(dest, ignore_errors=True)
        if not ok:
            raise RuntimeError(f"no matching files in the archive (looked for {', '.join(sorted(want))})")
        return rel

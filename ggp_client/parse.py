"""Pure parsing and naming helpers: no network, no database."""
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

BASE = "https://www.ggp-i.org"
CATALOG_URL = f"{BASE}/data-catalog/"

# slug key -> (project, round, series folder name)
SERIES = {
    "ggs1": ("GGS", 1, "GGS-I"),
    "ggs2": ("GGS", 2, "GGS-II"),
    "ffs": ("FFS", None, "FFS"),
}
NO_WAVE = {"thefamilyandfertilitysurvey"}  # FFS is one survey per country
# sections listed only on the logged-in data access page (/form/), with no dataset page: heading -> slug key
ACCESS_SECTIONS = {"Harmonized Histories": "hh", "Additional Datasets": "other"}

# ggs2-at-wave1, ggs2-at-wave1-v1-0 (older version), upcoming-ggs2-fr-wave1
SLUG_RE = re.compile(
    r"(?P<upcoming>upcoming-)?(?P<key>[a-z0-9]+)-(?P<cc>[a-z]{2})-"
    r"(?P<wave>[a-z0-9]+?)(?:-v(?P<ver>\d+-\d+))?"
)
DOWNLOAD_HREF = re.compile(r"/form/download/(\d+)")
SIZE_RE = re.compile(r"(.+?),\s*([\d.]+)\s*([KMG]B)\b", re.S)
UNIT_MB = {"KB": 1 / 1024, "MB": 1, "GB": 1024}


def page_url(slug):
    return f"{BASE}/dataset/{slug}/"


def many(v):
    """None -> [] (no filter), one value -> [value], list/tuple/set -> list."""
    return [] if v is None else [v] if isinstance(v, (str, int)) else list(v)


def slug_from_href(href):
    """Dataset slug from a link, or None. The strict charset keeps slugs safe as path parts."""
    u = urlparse(href)
    if u.netloc not in ("", "www.ggp-i.org", "ggp-i.org"):
        return None
    m = re.fullmatch(r"/dataset/([A-Za-z0-9_-]+)/?", u.path)
    return m[1].lower() if m else None


def parse_slug(slug):
    key, _, name = slug.partition("-")
    if key in ACCESS_SECTIONS.values():  # hh-..., other-...: no country or wave, the name stands in for the wave
        return dict(key=key, project=key.upper(), round=None, series=key.upper(), country="",
                    wave=name, version=None, upcoming=False)
    m = SLUG_RE.fullmatch(slug)
    if not m:  # unknown pattern: still indexed, under OTHER
        return dict(key=None, project="OTHER", round=None, series="OTHER", country="",
                    wave=slug, version=None, upcoming=False)
    project, rnd, series = SERIES.get(m["key"], (m["key"].upper(), None, m["key"].upper()))
    wave = "" if m["wave"] in NO_WAVE else re.sub(r"^wave", "w", m["wave"])
    return dict(key=m["key"], project=project, round=rnd, series=series, country=m["cc"].upper(),
                wave=wave, version=m["ver"].replace("-", ".") if m["ver"] else None,
                upcoming=bool(m["upcoming"]))


def naming(p, slug, version):
    """Folder and file stem, both lowercase, e.g. ('ggs-ii/w1/at', 'ggs2_w1_at_v1.1')."""
    v = f"_v{version}" if version else ""
    if p["key"] is None:
        return "other", slug + v
    stem = "_".join(x for x in (p["key"], p["wave"], p["country"].lower()) if x) + v
    return "/".join(x for x in (p["series"], p["wave"], p["country"]) if x).lower(), stem


def sort_key(slug):
    p = parse_slug(slug)
    return (p["project"], p["round"] or 0, p["wave"], p["country"], slug)


def parse_catalog(html):
    """All dataset slugs linked from the catalogue page, in page order."""
    slugs = {}
    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        s = slug_from_href(a["href"])
        if s:
            slugs[s] = None
    return list(slugs)


def _field(text, label):
    m = re.search(rf"(?m)^[ \t]*{label}:\s*([^\n]+)", text)
    return m[1].strip() if m else None


def _date(s):
    for fmt in ("%A, %B %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return s.strip()


def _downloads(soup):
    files = {}
    for a in soup.find_all("a", href=DOWNLOAD_HREF):
        did = int(DOWNLOAD_HREF.search(a["href"])[1])
        label = size = None
        for el in a.parents:  # nearest ancestor that holds this one link plus a size
            if len(el.find_all("a", href=DOWNLOAD_HREF)) > 1:
                break
            txt = " ".join(el.get_text(" ").split()).rsplit("Downloads:", 1)[-1]
            m = SIZE_RE.search(txt)
            if m:
                label = m[1].strip() if len(m[1].strip()) <= 200 else None
                size = round(float(m[2]) * UNIT_MB[m[3]], 2)
                break
        files[did] = dict(download_id=did, url=urljoin(BASE, a["href"]), label=label, size_mb=size)
    return list(files.values())


def parse_dataset(html, slug):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    h1 = soup.find("h1")
    title = " ".join(h1.get_text().split()) if h1 else None

    v = re.match(r"\d+(?:\.\d+)*", _field(text, "Version") or "")
    version = v[0] if v else None
    if not version and title:
        t = re.search(r"\bv(\d+(?:\.\d+)+)\s*$", title)
        version = t[1] if t else None

    dates = re.split(r"\s+-\s+", _field(text, "Data Collection Date") or "")
    links = soup.find_all("a", href=re.compile(r"colectica\.org/item", re.I))
    colectica = next((a["href"] for a in links if "explore" in a.get_text().lower()),
                     links[0]["href"] if links else None)

    base = re.sub(r"-v\d+-\d+$", "", slug)  # other versions of the same dataset
    others = {s for a in soup.find_all("a", href=True)
              if (s := slug_from_href(a["href"])) and s != slug
              and re.fullmatch(re.escape(base) + r"(-v\d+-\d+)?", s)}

    return dict(
        title=title,
        country_name=re.split(r"\s+[–-]\s+", title, maxsplit=1)[0] if title else None,
        version=version,
        collected_from=_date(dates[0]) if dates[0] else None,
        collected_to=_date(dates[1]) if len(dates) > 1 else None,
        colectica_url=colectica,
        files=_downloads(soup),
        other_versions=sorted(others),
    )


def parse_access(html):
    """Datasets listed only on the logged-in data access page, by slug: one per heading, one file per row.

    These sections are hidden behind a "Show N datasets" button, but the rows are in the HTML.
    Each list appears twice (all, then split in columns), so rows are merged by download id.
    """
    found = {}
    for h2 in BeautifulSoup(html, "html.parser").find_all("h2"):
        key = ACCESS_SECTIONS.get(" ".join(h2.get_text().split()))
        if not key:
            continue
        for h3 in (h2.find_parent("section") or h2.parent).find_all("h3"):
            title = " ".join(h3.get_text().split())
            slug = f"{key}-" + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
            d = found.setdefault(slug, dict(
                title=title, country_name=None, version=None, collected_from=None, collected_to=None,
                colectica_url=None, page_url=f"{BASE}/form/", files={}, other_versions=[]))
            for li in h3.find_next("ul").find_all("li"):
                a = li.find("a", href=DOWNLOAD_HREF)
                if a:
                    did = int(DOWNLOAD_HREF.search(a["href"])[1])
                    label = " ".join(li.div.get_text().split()) if li.div else None
                    d["files"][did] = dict(download_id=did, url=urljoin(BASE, a["href"]), label=label, size_mb=None)
    for d in found.values():
        d["files"] = list(d["files"].values())
    return found

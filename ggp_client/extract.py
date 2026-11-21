"""Unpack downloaded archives: keep the datafiles asked for, mirror nested zips as nested folders."""
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from . import parse

DATA_EXT = ("dta", "sav", "csv")
MAX_DEPTH = 5  # shortcut: no cap on unpacked size, fine for archives from the portal; add one if that changes


def norm_ext(ext):
    """One extension, a list, or None for all datafile types; any case, with or without a dot."""
    return {str(v).lower().lstrip(".") for v in parse.many(ext) or DATA_EXT}


def wanted(suffix, ext, metadata):
    """Keep the requested datafiles; with metadata also everything that is not a datafile type."""
    return suffix in ext or (metadata and suffix not in DATA_EXT)


def _parts(name):
    """Archive member path as folder/file parts, or None if it could escape the target folder."""
    p = PurePosixPath(name.replace("\\", "/"))
    if p.is_absolute() or ".." in p.parts or ":" in (p.parts[0] if p.parts else ""):
        return None
    return p.parts or None


def unpack(zf, dest, ext, metadata, depth=0):
    """Extract the wanted members of zf into dest; a nested zip becomes a folder named after it.

    Folder names are lowercased, file names are kept. Returns the number of files written.
    """
    n = 0
    for info in zf.infolist():
        parts = _parts(info.filename)
        if info.is_dir() or not parts or "__MACOSX" in parts or parts[-1].startswith("._"):
            continue
        *dirs, fname = parts
        folder = dest.joinpath(*(d.lower() for d in dirs))
        suffix = Path(fname).suffix.lstrip(".").lower()
        if suffix == "zip" and depth < MAX_DEPTH:
            with tempfile.TemporaryFile() as tmp:
                with zf.open(info) as src:
                    shutil.copyfileobj(src, tmp)
                with zipfile.ZipFile(tmp) as inner:
                    n += unpack(inner, folder / Path(fname).stem.lower(), ext, metadata, depth + 1)
        elif wanted(suffix, ext, metadata):
            folder.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(folder / fname, "wb") as out:
                shutil.copyfileobj(src, out)
            n += 1
    return n


def unpack_file(path, name, dest, ext, metadata):
    """Unpack a downloaded file: a zip is extracted, anything else is kept if it matches."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            return unpack(zf, dest, ext, metadata)
    parts = _parts(name)
    fname = parts[-1] if parts else "download"
    if not wanted(Path(fname).suffix.lstrip(".").lower(), ext, metadata):
        return 0
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, dest / fname)
    return 1

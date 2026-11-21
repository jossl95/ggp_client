"""Unpack downloaded archives: keep the datafiles asked for, mirror nested archives as nested folders."""
import os
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

import py7zr

from . import parse

DATA_EXT = ("dta", "sav", "csv")
ARCHIVE_EXT = ("zip", "7z")  # the portal serves both, and some ".7z" files are really zips: content decides
MAX_DEPTH = 5  # shortcut: no cap on unpacked size, fine for archives from the portal; add one if that changes


def norm_ext(ext):
    """One extension, a list, or None for all datafile types; any case, with or without a dot."""
    return {str(v).lower().lstrip(".") for v in parse.many(ext) or DATA_EXT}


def wanted(suffix, ext, metadata):
    """Keep the requested datafiles; with metadata also everything that is not a datafile type."""
    return suffix in ext or (metadata and suffix not in DATA_EXT)


def _suffix(name):
    return Path(name).suffix.lstrip(".").lower()


def _parts(name):
    """Archive member path as folder/file parts, or None if it could escape the target folder."""
    p = PurePosixPath(name.replace("\\", "/"))
    if p.is_absolute() or ".." in p.parts or ":" in (p.parts[0] if p.parts else ""):
        return None
    parts = p.parts
    if not parts or "__MACOSX" in parts or parts[-1].startswith("._"):
        return None
    return parts


def is_archive(path):
    return zipfile.is_zipfile(path) or py7zr.is_7zfile(path)


def _members(path, ext, metadata):
    """(parts, open) for each safe member of the zip or 7z archive at path that may be needed."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                parts = None if info.is_dir() else _parts(info.filename)
                if parts:
                    yield parts, lambda info=info: zf.open(info)
    elif py7zr.is_7zfile(path):
        with py7zr.SevenZipFile(path) as z, tempfile.TemporaryDirectory() as tmp:
            names = {n: _parts(n) for n in z.getnames()}
            # only safe members, and only those we keep or that may hold more (nested archives)
            targets = [n for n, parts in names.items() if parts and (
                _suffix(n) in ARCHIVE_EXT or wanted(_suffix(n), ext, metadata))]
            if targets:
                z.extract(tmp, targets=targets)
            for n in targets:
                f = Path(tmp, *names[n])
                if f.is_file():
                    yield names[n], lambda f=f: open(f, "rb")


def _write(src, target):
    """Write via a temporary name: an interrupted run never leaves a half file that looks complete."""
    part = target.with_name(target.name + ".part")
    with open(part, "wb") as out:
        shutil.copyfileobj(src, out)
    os.replace(part, target)


def unpack(path, dest, ext, metadata, force=False, depth=0):
    """Extract the wanted members of the archive at path into dest; a nested archive becomes a folder named after it.

    Folder names are lowercased, file names are kept. A file that already exists is left as it is
    unless force. Returns the number of matching files (written or already there).
    """
    n = 0
    for parts, opener in _members(path, ext, metadata):
        *dirs, fname = parts
        folder = dest.joinpath(*(d.lower() for d in dirs))
        suffix = _suffix(fname)
        if suffix in ARCHIVE_EXT and depth < MAX_DEPTH:
            with tempfile.TemporaryDirectory() as tmp, opener() as src:
                inner = Path(tmp, "inner")
                _write(src, inner)
                if is_archive(inner):
                    n += unpack(inner, folder / Path(fname).stem.lower(), ext, metadata, force, depth + 1)
                    continue
        if wanted(suffix, ext, metadata):
            target = folder / fname
            if force or not target.exists():
                folder.mkdir(parents=True, exist_ok=True)
                with opener() as src:
                    _write(src, target)
            n += 1
    return n


def unpack_file(path, name, dest, ext, metadata, force=False):
    """Unpack a downloaded file: a zip or 7z is extracted, anything else is kept if it matches."""
    if is_archive(path):
        return unpack(path, dest, ext, metadata, force)
    parts = _parts(name)
    fname = parts[-1] if parts else "download"
    if not wanted(_suffix(fname), ext, metadata):
        return 0
    if force or not (dest / fname).exists():
        dest.mkdir(parents=True, exist_ok=True)
        with open(path, "rb") as src:
            _write(src, dest / fname)
    return 1

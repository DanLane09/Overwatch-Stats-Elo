# -*- mode: python ; coding: utf-8 -*-

import os
from PyInstaller.utils.hooks import collect_all

PROJECT_ROOT = os.path.abspath(os.path.join(SPECPATH, '..'))

datas = []
binaries = []
hiddenimports = []

for pkg in ['torch', 'torchvision', 'easyocr', 'PySide6']:
    pkg_datas, pkg_binaries, pkg_hiddenimports = collect_all(pkg)
    datas += pkg_datas
    binaries += pkg_binaries
    hiddenimports += pkg_hiddenimports

a = Analysis(
    [os.path.join(PROJECT_ROOT, 'src', 'main.py')],
    pathex=[os.path.join(PROJECT_ROOT, 'src')],
    binaries=binaries,
    datas=datas + [
        (os.path.join(PROJECT_ROOT, 'assets', 'ReadTimeModel.pth'), 'assets'),
        (os.path.join(PROJECT_ROOT, 'assets', 'Images'), 'assets/Images'),
    ],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

splash = Splash(
    os.path.join(PROJECT_ROOT, 'assets', 'splash.png'),
    binaries=a.binaries,
    datas=a.datas,
    text_pos=(10, 250),
    text_size=12,
    text_color='white',
)

exe = EXE(
    pyz,
    a.scripts,
    splash,                    # added — shows splash.png immediately on launch
    exclude_binaries=True,     # changed — leaves binaries out of the exe itself
    name='OWTVstats',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    splash.binaries,           # added — splash's own resources
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='OverwatchStatsElo',
)
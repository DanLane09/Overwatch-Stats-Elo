"""
Drop-in replacement for the bettercam camera object, built on windows_capture
(Windows Graphics Capture API).

Mimics the three calls used by the main script:
    camera.start()
    camera.get_latest_frame()   # blocks until a NEW frame arrives, returns RGB/BGR ndarray
    camera.stop()

Install:
    pip install windows-capture opencv-python numpy
"""
import ctypes
import threading
from ctypes import wintypes

import cv2
import numpy as np
from windows_capture import WindowsCapture, Frame, InternalCaptureControl

user32 = ctypes.windll.user32
dwmapi = ctypes.windll.dwmapi
DWMWA_EXTENDED_FRAME_BOUNDS = 9

try:  # make sure Win32 coordinates are physical pixels
    user32.SetProcessDPIAware()
except Exception:
    pass


def _find_hwnd(window_name: str) -> int:
    return user32.FindWindowW(None, window_name)


def _client_box(hwnd: int):
    """
    Returns (x, y, w, h) of the window's client area relative to the top-left
    of the captured window image (which includes title bar / borders).
    For borderless or fullscreen windows this is (0, 0, w, h).
    """
    pt = wintypes.POINT(0, 0)
    if not user32.ClientToScreen(hwnd, ctypes.byref(pt)):
        return None
    client = wintypes.RECT()
    if not user32.GetClientRect(hwnd, ctypes.byref(client)):
        return None
    frame = wintypes.RECT()
    if dwmapi.DwmGetWindowAttribute(
        hwnd, DWMWA_EXTENDED_FRAME_BOUNDS, ctypes.byref(frame), ctypes.sizeof(frame)
    ) != 0:
        return None
    return pt.x - frame.left, pt.y - frame.top, client.right, client.bottom


class WindowCapture:
    def __init__(
        self,
        window_name: str | None = None,
        monitor_index: int | None = None,
        output_color: str = "RGB",
        crop_client: bool = True,
        cursor_capture: bool = False,
        draw_border: bool = False,
        first_frame_timeout: float = 5.0,
        frame_timeout: float = 1.0,
    ):
        if (window_name is None) == (monitor_index is None):
            raise ValueError("Provide exactly one of window_name or monitor_index")
        if output_color not in ("RGB", "BGR"):
            raise ValueError("output_color must be 'RGB' or 'BGR'")

        self._window_name = window_name
        self._monitor_index = monitor_index
        self._rgb = output_color == "RGB"
        self._crop_client = crop_client and window_name is not None
        self._cursor_capture = cursor_capture
        self._draw_border = draw_border
        self._first_frame_timeout = first_frame_timeout
        self._frame_timeout = frame_timeout

        self._cond = threading.Condition()
        self._control = None
        self._frame = None
        self._seq = 0
        self._last_seq = 0
        self._closed = False

    # ------------------------------------------------------------------ #
    def start(self):
        if self._control is not None:
            return

        with self._cond:
            self._frame = None
            self._seq = 0
            self._last_seq = 0
            self._closed = False

        kwargs = {
            "cursor_capture": self._cursor_capture,
            "draw_border": self._draw_border,
        }
        if self._window_name is not None:
            kwargs["window_name"] = self._window_name
        else:
            kwargs["monitor_index"] = self._monitor_index

        capture = WindowsCapture(**kwargs)

        @capture.event
        def on_frame_arrived(frame: Frame, ctrl: InternalCaptureControl):
            self._on_frame(frame)

        @capture.event
        def on_closed():
            with self._cond:
                self._closed = True
                self._cond.notify_all()

        # A new WindowsCapture is created on every start(), so start/stop can be
        # repeated once per map exactly like the bettercam camera.
        self._control = capture.start_free_threaded()

    def stop(self):
        control, self._control = self._control, None
        if control is not None:
            try:
                control.stop()
            except Exception:
                pass
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def get_latest_frame(self) -> np.ndarray:
        """Block until a frame newer than the last one returned is available."""
        with self._cond:
            timeout = self._first_frame_timeout if self._frame is None else self._frame_timeout
            self._cond.wait_for(
                lambda: self._seq != self._last_seq or self._closed, timeout=timeout
            )
            if self._frame is None:
                raise TimeoutError(
                    "No frame received. Is the window title exact, and is the window "
                    "not minimised?"
                )
            # If we timed out waiting (static screen / window closed) we return the
            # most recent frame rather than hanging the main loop.
            self._last_seq = self._seq
            return self._frame

    # ------------------------------------------------------------------ #
    def _on_frame(self, frame: Frame):
        buf = frame.frame_buffer  # (h, w, 4) BGRA

        if self._crop_client:
            hwnd = _find_hwnd(self._window_name)
            box = _client_box(hwnd) if hwnd else None
            if box is not None:
                x, y, w, h = box
                fh, fw = buf.shape[:2]
                x0, y0 = max(x, 0), max(y, 0)
                x1, y1 = min(x + w, fw), min(y + h, fh)
                if x1 > x0 and y1 > y0 and (x0, y0, x1, y1) != (0, 0, fw, fh):
                    buf = buf[y0:y1, x0:x1]

        # cvtColor allocates a new array, so no extra copy is needed and the
        # callback's buffer is never held after it returns.
        code = cv2.COLOR_BGRA2RGB if self._rgb else cv2.COLOR_BGRA2BGR
        out = cv2.cvtColor(np.ascontiguousarray(buf), code)

        with self._cond:
            self._frame = out
            self._seq += 1
            self._cond.notify_all()
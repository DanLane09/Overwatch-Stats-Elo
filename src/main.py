import sys
from PySide6.QtCore import QObject, Signal, Slot, QThread, Qt
from PySide6.QtWidgets import QApplication, QFrame, QHBoxLayout, QLabel, QMainWindow, QPushButton, QVBoxLayout, QWidget
import reading_scoreboard_replay
import requests, os, subprocess, shutil
from config import resource_path, hash_file
from version import __version__

GITHUB_REPO = "DanLane09/Overwatch-Stats-Elo"

def version_tuple(v):
    return tuple(int(x) for x in v.split("."))

def check_for_update():
    try:
        resp = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest", timeout=5
        )
        resp.raise_for_status()
        release = resp.json()
        latest = release["tag_name"].lstrip("v")
        if version_tuple(latest) > version_tuple(__version__):
            return release
    except Exception:
        pass  # network hiccup — don't block the app over this
    return None

def ensure_updater_available(updater_url, updater_expected_hash):
    updater_dir = os.path.join(os.environ["LOCALAPPDATA"], "OverwatchStatsElo", "updater")
    os.makedirs(updater_dir, exist_ok=True)
    updater_path = os.path.join(updater_dir, "updater.exe")

    needs_download = True
    if os.path.exists(updater_path):
        if hash_file(updater_path) == updater_expected_hash:
            needs_download = False

    if needs_download:
        resp = requests.get(updater_url, timeout=30)
        resp.raise_for_status()
        with open(updater_path, "wb") as f:
            f.write(resp.content)

    return updater_path

def launch_updater(release):
    install_dir = os.path.dirname(sys.executable)
    assets = {a["name"]: a for a in release["assets"]}
    updater_path = ensure_updater_available(
        assets["updater.exe"]["browser_download_url"],
        assets["updater.exe"]["digest"].removeprefix("sha256:"),
    )

    subprocess.Popen([
        updater_path,
        "--install-dir", install_dir,
        "--manifest-url", assets["manifest.json"],
        "--zip-url", assets["OverwatchStatsElo.zip"],
        "--relaunch", sys.executable,
    ])
    sys.exit(0)



class ProcessingState(QObject):
    # Signals sent from the scoreboard reader to the UI.
    map_changed = Signal(str, str)
    time_changed = Signal(str)
    status_changed = Signal(str)
    running_changed = Signal(bool)
    error_occurred = Signal(str)


# Reader worker
class ReaderWorker(QObject):
    finished = Signal()
    completed = Signal()
    error = Signal(str)

    def __init__(self, state):
        super().__init__()
        self.state = state

    @Slot()
    def run(self):
        try:
            self.state.running_changed.emit(True)
            reading_scoreboard_replay.run_reader(state=self.state)
            self.completed.emit()
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self.state.running_changed.emit(False)
            self.finished.emit()


# Main window
class MainWindow(QMainWindow):

    def __init__(self):

        super().__init__()

        self.setWindowTitle("Overwatch Scoreboard Reader")
        self.setMinimumSize(760, 560)
        self.resize(820, 620)

        self.thread = None
        self.worker = None

        self.state = ProcessingState()

        self.setup_ui()
        self.setup_connections()
        self.apply_stylesheet()

    # UI
    def setup_ui(self):
        central = QWidget()
        central.setObjectName("centralWidget")

        self.setCentralWidget(central)

        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(34, 30, 34, 30,)
        main_layout.setSpacing(18)

        # Header
        header_layout = QHBoxLayout()
        header_layout.setSpacing(12)

        mark = QLabel("◢")
        mark.setObjectName("brandMark")

        header_layout.addWidget(mark, alignment=Qt.AlignmentFlag.AlignTop)

        title_layout = QVBoxLayout()
        title_layout.setSpacing(1)

        title = QLabel("OVERWATCH")
        title.setObjectName("title")

        subtitle = QLabel("SCOREBOARD READER")
        subtitle.setObjectName("subtitle")

        title_layout.addWidget(title)
        title_layout.addWidget(subtitle)

        header_layout.addLayout(title_layout)
        header_layout.addStretch()

        # Status indicator
        self.header_status = QLabel("● READY")
        self.header_status.setObjectName("headerStatus")
        header_layout.addWidget(self.header_status, alignment=Qt.AlignmentFlag.AlignTop)
        main_layout.addLayout(header_layout)

        # Cyan separator beneath header
        header_line = QFrame()
        header_line.setObjectName("headerLine")
        header_line.setFixedHeight(2)

        main_layout.addWidget(header_line)

        # Instructions
        instructions_card = QFrame()
        instructions_card.setObjectName("card")
        instructions_layout = QVBoxLayout(instructions_card)
        instructions_layout.setContentsMargins(20, 16, 20, 16,)
        instructions_layout.setSpacing(7)
        instructions_title = QLabel("INSTRUCTIONS")
        instructions_title.setObjectName("sectionTitle")

        instructions_text = QLabel(
            "Make sure you have colours set to DEFAULT BLUE AND DEFAULT RED. "
            "Open the Replays tab in your career profile. "
            "Press RUN to begin processing the replays. "
            "Tab back into the game window. "
            "The current map and game time will be displayed below."
        )

        instructions_text.setObjectName("instructions")
        instructions_text.setWordWrap(True)
        instructions_layout.addWidget(instructions_title)
        instructions_layout.addWidget(instructions_text)
        main_layout.addWidget(instructions_card)

        # Current map card
        map_card = QFrame()
        map_card.setObjectName("mapCard")
        map_layout = QVBoxLayout(map_card)
        map_layout.setContentsMargins(24, 19, 24, 23,)
        map_layout.setSpacing(5)
        map_header = QHBoxLayout()
        current_map_title = QLabel("CURRENT MAP")
        current_map_title.setObjectName("sectionTitle")
        map_header.addWidget(current_map_title)
        map_header.addStretch()

        self.map_type_label = QLabel("--")
        self.map_type_label.setObjectName("mapType")
        map_header.addWidget(self.map_type_label)
        map_layout.addLayout(map_header)
        self.map_label = QLabel("Waiting to start")
        self.map_label.setObjectName("mapLabel")
        self.map_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        self.map_label.setWordWrap(True)
        map_layout.addWidget(self.map_label)
        main_layout.addWidget(map_card)

        # Time card
        time_card = QFrame()
        time_card.setObjectName("timeCard")
        time_layout = QVBoxLayout(time_card)
        time_layout.setContentsMargins(24, 17, 24, 21,)
        time_layout.setSpacing(0)
        time_title = QLabel("MAP TIME")
        time_title.setObjectName("sectionTitle")
        time_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        time_layout.addWidget(time_title)
        self.time_label = QLabel("--:--")
        self.time_label.setObjectName("timeLabel")
        self.time_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        time_layout.addWidget(self.time_label)

        main_layout.addWidget(time_card)

        # Status
        status_layout = QHBoxLayout()
        status_layout.setSpacing(9)
        status_caption = QLabel("STATUS")
        status_caption.setObjectName("statusCaption")

        self.status_indicator = QLabel("●")
        self.status_indicator.setObjectName("statusIndicator")
        self.status_label = QLabel("Ready to process")
        self.status_label.setObjectName("statusLabel")

        status_layout.addWidget(status_caption)
        status_layout.addWidget(self.status_indicator)
        status_layout.addWidget(self.status_label)
        status_layout.addStretch()
        main_layout.addLayout(status_layout)
        main_layout.addStretch()

        # Run button
        self.run_button = QPushButton("RUN")
        self.run_button.setObjectName("runButton")
        self.run_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.run_button.setMinimumHeight(62)
        main_layout.addWidget(self.run_button)

    # Signals
    def setup_connections(self):
        self.run_button.clicked.connect(self.start_reader)
        self.state.map_changed.connect(self.update_map)
        self.state.time_changed.connect(self.update_time)
        self.state.status_changed.connect(self.update_status)
        self.state.running_changed.connect(self.update_running)
        self.state.error_occurred.connect(self.show_error)

    # UI updates
    @Slot(str, str)
    def update_map(self, map_name, map_type):
        self.map_label.setText(map_name)
        self.map_type_label.setText(map_type.upper())

    @Slot(str)
    def update_time(self, time_string):
        self.time_label.setText(time_string)

    @Slot(str)
    def update_status(self, status):
        self.status_label.setText(status)

    @Slot(bool)
    def update_running(self, running):
        self.run_button.setEnabled(not running)
        if running:
            self.run_button.setText("PROCESSING")
            self.header_status.setText("● PROCESSING")
            self.header_status.setObjectName("headerStatusRunning")
            self.status_indicator.setObjectName("statusIndicatorRunning")

        else:
            self.run_button.setText("RUN")
            self.header_status.setText("● READY")
            self.header_status.setObjectName("headerStatus")
            self.status_indicator.setObjectName("statusIndicator")

        self.refresh_widget_style(self.header_status)
        self.refresh_widget_style(self.status_indicator)

    @Slot(str)
    def show_error(self, message):
        self.status_label.setText(f"Error: {message}")
        self.header_status.setText("● ERROR")
        self.header_status.setObjectName("headerStatusError")
        self.status_indicator.setObjectName("statusIndicatorError")
        self.refresh_widget_style(self.header_status)
        self.refresh_widget_style(self.status_indicator)

    def refresh_widget_style(self, widget):
        widget.style().unpolish(widget)
        widget.style().polish(widget)
        widget.update()

    # Start reader
    def start_reader(self):
        if self.thread is not None:
            return

        # Reset display
        self.map_label.setText("Preparing...")
        self.map_type_label.setText("--")
        self.time_label.setText("--:--")
        self.status_label.setText("Starting scoreboard reader...")
        self.status_indicator.setObjectName("statusIndicatorRunning")

        self.refresh_widget_style(self.status_indicator)
        # Create thread
        self.thread = QThread()
        # Create worker
        self.worker = ReaderWorker(self.state)
        # Move worker into thread
        self.worker.moveToThread(self.thread)
        # Start worker when thread starts
        self.thread.started.connect(self.worker.run)
        # Worker finished -> stop thread
        self.worker.finished.connect(self.thread.quit)
        self.worker.completed.connect(self.reader_completed)
        # Clean up worker
        self.worker.finished.connect(self.worker.deleteLater)
        # Clean up thread
        self.thread.finished.connect(self.thread.deleteLater)
        # Errors
        self.worker.error.connect(self.show_error)
        # Final cleanup
        self.thread.finished.connect(self.reader_finished)

        # Start
        self.thread.start()

    def reader_completed(self):
        self.status_label.setText("Processing complete")
        self.status_indicator.setObjectName("statusIndicator")
        self.refresh_widget_style(self.status_indicator)

    def reader_finished(self):
        self.thread = None
        self.worker = None

        self.status_indicator.setObjectName("statusIndicator")
        self.refresh_widget_style(self.status_indicator)

    # Styling
    def apply_stylesheet(self):

        self.setStyleSheet("""

        /* ===================================================
           Base
           =================================================== */

        QWidget#centralWidget {
            background: #0b0e10;
        }

        QLabel {
            color: #e8eeee;
            font-family: "Segoe UI";
        }


        /* ===================================================
           Header
           =================================================== */

        QLabel#brandMark {
            color: #0cf2f2;
            font-family: "Segoe UI";
            font-size: 29px;
            font-weight: 900;
            padding-top: 1px;
        }

        QLabel#title {
            color: #f4f7f7;
            font-size: 27px;
            font-weight: 800;
            letter-spacing: 1px;
        }

        QLabel#subtitle {
            color: #687276;
            font-size: 10px;
            font-weight: 700;
            letter-spacing: 2.5px;
        }

        QLabel#headerStatus {
            color: #697477;
            font-size: 10px;
            font-weight: 700;
            letter-spacing: 1.5px;
        }

        QLabel#headerStatusRunning {
            color: #0cf2f2;
            font-size: 10px;
            font-weight: 700;
            letter-spacing: 1.5px;
        }

        QLabel#headerStatusError {
            color: #ff5c5c;
            font-size: 10px;
            font-weight: 700;
            letter-spacing: 1.5px;
        }

        QFrame#headerLine {
            background: #0cf2f2;
        }


        /* ===================================================
           Cards
           =================================================== */

        QFrame#card {
            background: #15181c;
            border: 1px solid #252a2f;
            border-radius: 5px;
        }

        QFrame#mapCard {
            background: #15181c;
            border: 1px solid #252a2f;
            border-radius: 5px;
        }

        QFrame#timeCard {
            background: #111417;
            border: 1px solid #252a2f;
            border-radius: 5px;
        }


        /* ===================================================
           Section titles
           =================================================== */

        QLabel#sectionTitle {
            color: #687478;
            font-size: 10px;
            font-weight: 800;
            letter-spacing: 1.7px;
        }


        /* ===================================================
           Instructions
           =================================================== */

        QLabel#instructions {
            color: #aeb8ba;
            font-size: 12px;
        }


        /* ===================================================
           Map
           =================================================== */

        QLabel#mapLabel {
            color: #f2f5f5;
            font-size: 28px;
            font-weight: 600;
            padding-top: 7px;
        }

        QLabel#mapType {
            background: #102d30;
            color: #0cf2f2;
            border: 1px solid #0b5c61;
            border-radius: 3px;
            padding: 5px 10px;
            font-size: 9px;
            font-weight: 800;
            letter-spacing: 1.2px;
        }


        /* ===================================================
           Timer
           =================================================== */

        QLabel#timeLabel {
            color: #f4f7f7;
            font-family: "Consolas";
            font-size: 62px;
            font-weight: 700;
            padding-top: 3px;
        }


        /* ===================================================
           Status
           =================================================== */

        QLabel#statusCaption {
            color: #596468;
            font-size: 9px;
            font-weight: 800;
            letter-spacing: 1.6px;
        }

        QLabel#statusIndicator {
            color: #0cf2f2;
            font-size: 8px;
        }

        QLabel#statusIndicatorRunning {
            color: #0cf2f2;
            font-size: 8px;
        }

        QLabel#statusIndicatorError {
            color: #ff5c5c;
            font-size: 8px;
        }

        QLabel#statusLabel {
            color: #99a5a8;
            font-size: 11px;
        }


        /* ===================================================
           Run button
           =================================================== */

        QPushButton#runButton {
            background: #0cf2f2;
            color: #111417;
            border: none;
            border-radius: 4px;

            font-family: "Segoe UI";
            font-size: 13px;
            font-weight: 900;

            letter-spacing: 2.5px;
        }

        QPushButton#runButton:hover {
            background: #35f6f6;
        }

        QPushButton#runButton:pressed {
            background: #08caca;
        }

        QPushButton#runButton:disabled {
            background: #1d292b;
            color: #596568;
        }

        """)

# Application
def main():
    release = check_for_update()
    if release:
        launch_updater(release)

    app = QApplication(sys.argv)
    app.setApplicationName(
        "Overwatch Scoreboard Reader"
    )
    window = MainWindow()
    if getattr(sys, 'frozen', False):
        import pyi_splash
        pyi_splash.close()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
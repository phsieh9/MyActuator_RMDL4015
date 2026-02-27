"""
MyActuator RMD-L-4015  —  Dual Motor Control Panel
====================================================
Two Canable adapters, one per motor, each on its own independent CAN bus.

Requirements
------------
    pip install python-can gs_usb

Canable setup (do this for EACH adapter)
-----------------------------------------
  1. Flash Candlelight firmware:
       https://canable.io/updater/canable1.html
       Open in Chrome, click Connect, select adapter, click Flash Candlelight.

  2. Install WinUSB driver with Zadig:
       https://zadig.akeo.ie
       Options > List All Devices > select "Canable" or "GS_USB"
       Set driver to WinUSB > Replace Driver

  3. pip install gs_usb

CAN channel numbering (gs_usb)
-------------------------------
  With two Canable adapters plugged in, they get channel indices 0 and 1
  in the order Windows enumerates them.

  MOTOR_1_CHANNEL = 0   <- first  Canable plugged in -> Motor 1
  MOTOR_2_CHANNEL = 1   <- second Canable plugged in -> Motor 2

  If the motors appear swapped, swap the channel numbers below.
  Both motors can have CAN ID = 1 because each is on its own isolated bus.

Wiring per adapter
------------------
  Canable screw terminals:
    CAN H  ->  Motor CAN H
    CAN L  ->  Motor CAN L
    GND    ->  Motor GND  (recommended)
  Enable the 120 ohm termination jumper on the Canable board.
"""

# ============================================================
# CAN adapter  —  TWO Canable adapters, one per motor
# ============================================================
CAN_INTERFACE    = "gs_usb"
CAN_BITRATE      = 1_000_000

MOTOR_1_CHANNEL  = 0   # Canable connected to Motor 1
MOTOR_2_CHANNEL  = 1   # Canable connected to Motor 2

# Both motors use CAN node ID 1 — each is on its own isolated bus
MOTOR_1_ID       = 1
MOTOR_2_ID       = 1

# Homing defaults
HOMING_SPEED_DPS    = 20.0
HOMING_STALL_AMPS   = 0.35    # amps to detect hard stop (free-run ~0.11A)
HOMING_STALL_WINDOW = 0.2     # seconds current must stay high to confirm stall
HOMING_TIMEOUT_S    = 30.0
HOMING_RETRACT_DEG  = 0.5     # retract 0.5 deg after hitting hard stop
HOMING_DIRECTION    = -1      # -1 = CW,  +1 = CCW

# ============================================================
import tkinter as tk
from tkinter import messagebox
import threading
import time
import queue
import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Callable

try:
    import can
    CAN_AVAILABLE = True
except ImportError:
    CAN_AVAILABLE = False

import json, os

_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "motor_config.json")
_DEFAULT_CONFIG = {"motor1_default_pos": 0.0, "motor2_default_pos": 0.0}

def _load_config() -> dict:
    try:
        with open(_CONFIG_FILE) as f:
            cfg = json.load(f)
        # fill any missing keys
        for k, v in _DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)
        return cfg
    except Exception:
        return dict(_DEFAULT_CONFIG)

def _save_config(cfg: dict):
    try:
        with open(_CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Config save error: {e}")


# ============================================================
# Motor driver
# ============================================================

class CMD(IntEnum):
    READ_MOTOR_STATUS_1    = 0x9A
    READ_MOTOR_STATUS_2    = 0x9C
    MOTOR_OFF              = 0x80
    MOTOR_STOP             = 0x81
    MOTOR_RUNNING          = 0x88
    SPEED_CONTROL          = 0xA2
    POSITION_CONTROL_1     = 0xA3   # absolute multi-turn position
    POSITION_CONTROL_2     = 0xA4   # incremental position (relative to current)
    POSITION_CONTROL_4     = 0xA6
    WRITE_CURRENT_POS_ZERO = 0x19
    READ_MULTI_TURN_ANGLE  = 0x92
    SET_ZERO_POSITION      = 0x64   # set current position as multi-turn zero
    READ_PID_RAM           = 0x30
    WRITE_PID_RAM          = 0x31
    WRITE_PID_ROM          = 0x32
    READ_ACCEL             = 0x42
    WRITE_ACCEL_RAM        = 0x43


@dataclass
class MotorStatus:
    temperature: int   = 0
    voltage:     float = 0.0
    error_state: int   = 0
    current:     float = 0.0
    speed:       float = 0.0
    angle:       float = 0.0


@dataclass
class PIDParams:
    current_kp:  int = 100
    current_ki:  int = 100
    speed_kp:    int = 100
    speed_ki:    int = 50
    position_kp: int = 100
    position_ki: int = 50


@dataclass
class AccelParams:
    acceleration_dps2: int = 500


class MotorDriver:
    BASE_ID  = 0x140
    REPLY_ID = 0x240

    def __init__(self, motor_id: int, bus, timeout: float = 0.25):
        self.motor_id = motor_id
        self.can_id   = self.BASE_ID  + motor_id
        self.reply_id = self.REPLY_ID + motor_id
        self.bus      = bus
        self.timeout  = timeout

    def _send(self, data: bytes):
        msg = can.Message(arbitration_id=self.can_id, data=data, is_extended_id=False)
        self.bus.send(msg)

    def _send_recv(self, data: bytes):
        """Send command and wait for reply. Accepts any reply from this motor."""
        # Drain stale frames
        while self.bus.recv(timeout=0.0) is not None:
            pass
        self._send(data)
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            msg = self.bus.recv(timeout=max(0.0, deadline - time.monotonic()))
            if msg and msg.arbitration_id == self.reply_id:
                return msg
        return None

    @staticmethod
    def _parse_s2(msg) -> MotorStatus:
        d = msg.data
        return MotorStatus(
            temperature=d[1],
            current=struct.unpack_from("<h", d, 2)[0] / 100.0,
            speed=float(struct.unpack_from("<h", d, 4)[0]),
            angle=struct.unpack_from("<H", d, 6)[0] / 100.0,
        )

    def motor_off(self):
        self._send(bytes([CMD.MOTOR_OFF,  0,0,0,0,0,0,0]))

    def motor_stop(self):
        self._send(bytes([CMD.MOTOR_STOP, 0,0,0,0,0,0,0]))

    def motor_running(self):
        """Enable motor torque (required before position/speed commands on some firmware)."""
        self._send(bytes([CMD.MOTOR_RUNNING, 0,0,0,0,0,0,0]))

    def set_speed(self, dps: float):
        spd = max(-2_147_483_648, min(2_147_483_647, int(dps * 100)))
        self._send(struct.pack("<Bxxxi", CMD.SPEED_CONTROL, spd))

    def set_position_multiturn(self, deg: float, max_dps: float = 360.0):
        self._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_1,
                               max(0, min(65535, int(max_dps))),
                               int(deg * 100)))

    def set_position_increment(self, inc: float, max_dps: float = 360.0):
        self._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_4,
                               max(0, min(65535, int(max_dps))),
                               int(inc * 100)))

    def read_status(self):
        r = self._send_recv(bytes([CMD.READ_MOTOR_STATUS_2, 0,0,0,0,0,0,0]))
        if r is None: return None
        d = bytes(r.data)
        if len(d) < 8: return None
        if d[0] == CMD.READ_MOTOR_STATUS_2:
            return MotorStatus(
                temperature=d[1],
                current=struct.unpack_from("<h", d, 2)[0] / 100.0,
                speed=float(struct.unpack_from("<h", d, 4)[0]),
                angle=struct.unpack_from("<H", d, 6)[0] / 100.0,
            )
        elif d[0] == CMD.READ_MOTOR_STATUS_1:
            return MotorStatus(
                temperature=d[1],
                voltage=struct.unpack_from("<H", d, 2)[0] / 10.0,
                error_state=struct.unpack_from("<H", d, 5)[0],
            )
        return None

    def read_status_1(self):
        r = self._send_recv(bytes([CMD.READ_MOTOR_STATUS_1, 0,0,0,0,0,0,0]))
        if r is None: return None
        d = r.data
        return MotorStatus(temperature=d[1],
                           voltage=struct.unpack_from("<H", d, 2)[0] / 10.0,
                           error_state=struct.unpack_from("<H", d, 5)[0])

    def read_single_turn_angle(self) -> Optional[float]:
        """Read single-turn angle (0-360) from STATUS_2 reply."""
        r = self._send_recv(bytes([CMD.READ_MOTOR_STATUS_2, 0,0,0,0,0,0,0]))
        if r is None: return None
        d = bytes(r.data)
        if len(d) < 8 or d[0] != CMD.READ_MOTOR_STATUS_2: return None
        single = struct.unpack_from("<H", d, 6)[0] / 100.0
        return self._update_angle(single)

    def read_multi_turn_angle(self) -> Optional[float]:
        """Alias for read_single_turn_angle with unwrapping."""
        return self.read_single_turn_angle()

    def reset_unwrap(self):
        """Reset multi-turn tracking (call after init)."""
        self._unwrap_ref = None
        self._unwrap_turns = 0
        self._last_angle = None

    def _update_angle(self, single: float) -> float:
        """Unwrap single-turn (0-360) into continuous multi-turn angle."""
        if not hasattr(self, '_unwrap_ref') or self._unwrap_ref is None:
            self._unwrap_ref = single
            self._unwrap_turns = 0
        else:
            diff = single - self._unwrap_ref
            if diff > 180.0:
                self._unwrap_turns -= 1
            elif diff < -180.0:
                self._unwrap_turns += 1
            self._unwrap_ref = single
        self._last_angle = single + self._unwrap_turns * 360.0
        return self._last_angle

    def set_zero_position(self):
        self._send(bytes([CMD.WRITE_CURRENT_POS_ZERO, 0,0,0,0,0,0,0]))

    # PID
    def read_pid(self):
        r = self._send_recv(bytes([CMD.READ_PID_RAM, 0,0,0,0,0,0,0]))
        if r is None: return None
        d = r.data
        return PIDParams(d[2], d[3], d[4], d[5], d[6], d[7])

    def write_pid_ram(self, p: PIDParams):
        r = self._send_recv(bytes([CMD.WRITE_PID_RAM, 0,
                                   p.current_kp, p.current_ki,
                                   p.speed_kp,   p.speed_ki,
                                   p.position_kp, p.position_ki]))
        if r is None: return None
        d = r.data
        return PIDParams(d[2], d[3], d[4], d[5], d[6], d[7])

    def write_pid_rom(self, p: PIDParams):
        r = self._send_recv(bytes([CMD.WRITE_PID_ROM, 0,
                                   p.current_kp, p.current_ki,
                                   p.speed_kp,   p.speed_ki,
                                   p.position_kp, p.position_ki]))
        if r is None: return None
        d = r.data
        return PIDParams(d[2], d[3], d[4], d[5], d[6], d[7])

    def read_acceleration(self):
        r = self._send_recv(bytes([CMD.READ_ACCEL, 0,0,0,0,0,0,0]))
        return AccelParams(struct.unpack_from("<I", r.data, 4)[0]) if r else None

    def write_acceleration_ram(self, dps2: int):
        r = self._send_recv(struct.pack("<BxxxI", CMD.WRITE_ACCEL_RAM, max(1, int(dps2))))
        return AccelParams(struct.unpack_from("<I", r.data, 4)[0]) if r else None


# ============================================================
# CAN manager
# ============================================================

class CANManager:
    """
    Manages two independent CAN buses — one Canable adapter per motor.

    Internal key scheme:
        self._buses[1]  = can.Bus for Motor 1  (channel MOTOR_1_CHANNEL)
        self._buses[2]  = can.Bus for Motor 2  (channel MOTOR_2_CHANNEL)
        self._drivers[1] = MotorDriver(node_id=MOTOR_1_ID, bus=bus1)
        self._drivers[2] = MotorDriver(node_id=MOTOR_2_ID, bus=bus2)

    The panel index (1 or 2) is used as the lookup key throughout, even
    though both motors may share the same CAN node ID (both = 1).
    """

    def __init__(self):
        self._buses:   dict[int, "can.Bus"]    = {}
        self._drivers: dict[int, MotorDriver]  = {}
        self._lock = threading.Lock()

    def connect(self) -> tuple[bool, str]:
        if not CAN_AVAILABLE:
            return False, "python-can not installed.\nRun:  pip install python-can gs_usb"

        try:
            from gs_usb.gs_usb import GsUsb
        except ImportError:
            return False, "gs_usb not installed.\nRun:  pip install gs_usb"

        # GsUsb.scan() may return different counts depending on OS/driver:
        #   4 entries (work PC): adapter A iface0(DFU), iface1(CAN), adapter B iface0, iface1
        #   2 entries (home PC): adapter A iface0(CAN), adapter B iface0(CAN)
        # Strategy: try to get 2 usable CAN devices by skipping DFU interfaces.
        all_devs = GsUsb.scan()
        n_all = len(all_devs)

        if n_all < 2:
            return False, (
                f"Found {n_all} gs_usb interface(s), need at least 2 (one per adapter).\n"
                "Check both UCAN adapters are plugged in with WinUSB driver via Zadig."
            )

        # Try to open each device to find the 2 working CAN interfaces.
        # DFU interfaces will fail to open or fail the first send — skip them.
        can_devs = []
        probe_errors = []
        for dev in all_devs:
            try:
                test_bus = can.Bus(
                    interface="gs_usb",
                    channel=dev.gs_usb.product,
                    bus=dev.gs_usb.bus,
                    address=dev.gs_usb.address,
                    bitrate=CAN_BITRATE,
                )
                can_devs.append((dev, test_bus))
                if len(can_devs) == 2:
                    break
            except Exception as e:
                probe_errors.append(f"  iface bus={dev.gs_usb.bus} addr={dev.gs_usb.address}: {e}")

        if len(can_devs) < 2:
            err_detail = "\n".join(probe_errors) if probe_errors else "  (no detail)"
            return False, (
                f"Found {n_all} gs_usb interface(s), opened {len(can_devs)} as CAN.\n"
                f"Open errors:\n{err_detail}\n\n"
                "Fix: run Zadig → Options → List All Devices\n"
                "Select each UCAN/USB2CAN entry → set driver to WinUSB → Install Driver\n"
                "Do this for ALL interfaces (repeat for each entry)."
            )

        errors = []
        with self._lock:
            # Close any previously open buses
            for panel_idx in list(self._buses.keys()):
                try: self._buses[panel_idx].shutdown()
                except Exception: pass
            self._buses.clear()
            self._drivers.clear()

            node_ids = [MOTOR_1_ID, MOTOR_2_ID]
            for panel_idx, (dev, bus), node_id in zip([1, 2], can_devs, node_ids):
                try:
                    self._buses[panel_idx]   = bus
                    self._drivers[panel_idx] = MotorDriver(node_id, bus)
                except Exception as e:
                    errors.append(f"Bus {panel_idx}: {e}")

        if errors and len(errors) == 2:
            return False, "Both CAN buses failed:\n" + "\n".join(errors)
        if errors:
            return True, f"Partial connect - one bus failed:\n{errors[0]}"
        # Warm up: flush CAN error-passive state and wake motors
        import time as _t
        for driver in self._drivers.values():
            for _ in range(8):
                try:
                    driver._send(bytes([CMD.MOTOR_RUNNING, 0,0,0,0,0,0,0]))
                    _t.sleep(0.03)
                except Exception:
                    pass
            # Drain any accumulated frames
            try:
                while driver.bus.recv(timeout=0.0) is not None:
                    pass
            except Exception:
                pass
        return True, (
            f"Both CAN buses connected  "
            f"(found {n_all} gs_usb interface(s), using 2 CAN interfaces)"
        )


    def get_motor(self, panel_idx: int) -> Optional[MotorDriver]:
        """Return the MotorDriver for panel 1 or panel 2."""
        return self._drivers.get(panel_idx)

    def _close_all(self):
        """Stop all motors and shut down both buses. Call with _lock held."""
        for driver in self._drivers.values():
            try: driver.motor_off()
            except Exception: pass
        for bus in self._buses.values():
            try: bus.shutdown()
            except Exception: pass
        self._buses.clear()
        self._drivers.clear()

    def shutdown(self):
        with self._lock:
            self._close_all()


# ============================================================
# Style constants
# ============================================================
BG        = "#0d0f14"
PANEL_BG  = "#13161e"
ACCENT    = "#00e5ff"
ACCENT2   = "#ff6b35"
GREEN     = "#39ff14"
RED       = "#ff3c38"
YELLOW    = "#ffd166"
WARN      = "#ff9f43"
TEXT      = "#e8eaf0"
TEXT_DIM  = "#5a6070"
BORDER    = "#1e2530"
BTN_BG    = "#1a1f2e"
BTN_HOV   = "#232a3a"

FT        = ("Calibri",  9)
FT_S      = ("Calibri",  8)
FT_BIG    = ("Calibri", 22, "bold")
FT_MONO   = ("Calibri", 14, "bold")
FT_HEAD   = ("Calibri", 11, "bold")
FT_TITLE  = ("Calibri", 13, "bold")


def _btn(parent, text, cmd, fg=ACCENT, width=18, state="normal"):
    b = tk.Button(parent, text=text, command=cmd,
                  font=FT, bg=BTN_BG, fg=fg,
                  activebackground=BTN_HOV, activeforeground=fg,
                  relief="flat", bd=0, width=width, cursor="hand2",
                  highlightthickness=1, highlightbackground=fg, state=state)
    b.bind("<Enter>", lambda e: b.config(bg=BTN_HOV))
    b.bind("<Leave>", lambda e: b.config(bg=BTN_BG))
    return b


def _lbl(parent, text, fg=TEXT, font=FT, anchor="w", bg=PANEL_BG):
    return tk.Label(parent, text=text, fg=fg, bg=bg, font=font, anchor=anchor)


def _sep(parent):
    return tk.Frame(parent, bg=BORDER, height=1)


def _entry(parent, var, width=8, fg=ACCENT):
    return tk.Entry(parent, textvariable=var, width=width,
                    bg=BTN_BG, fg=fg, insertbackground=fg,
                    relief="flat", font=FT,
                    highlightthickness=1, highlightbackground=fg)


# ============================================================
# Parameter popup
# ============================================================

class SettingsPanel(tk.Toplevel):
    """Lightweight settings dialog — always available, no motor connection needed."""

    def __init__(self, parent, motor_id: int, log_cb: Callable):
        super().__init__(parent)
        self.motor_id = motor_id
        self.log      = log_cb
        self.title(f"Motor {motor_id}  -  Settings")
        self.configure(bg=BG)
        self.resizable(False, False)
        self.grab_set()
        self._build()
        self.update_idletasks()
        px = parent.winfo_rootx()
        py = parent.winfo_rooty()
        self.geometry(f"+{px + 40}+{py + 40}")

    def _build(self):
        hdr = tk.Frame(self, bg=BG)
        hdr.pack(fill="x", padx=16, pady=(14, 6))
        tk.Label(hdr, text=f"MOTOR {self.motor_id}  —  SETTINGS",
                 fg=TEXT, bg=BG, font=FT_HEAD).pack(side="left")

        card = tk.Frame(self, bg=PANEL_BG, padx=16, pady=14)
        card.pack(fill="x", padx=16, pady=(0, 14))

        tk.Label(card, text="DEFAULT POSITION ON INIT  (absolute encoder degrees)",
                 fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).grid(
                     row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

        cfg = _load_config()
        key = f"motor{self.motor_id}_default_pos"

        tk.Label(card, text="Position (deg)", fg=TEXT, bg=PANEL_BG,
                 font=FT, anchor="w", width=18).grid(row=1, column=0, padx=(0,8), pady=4, sticky="w")

        self._var = tk.StringVar(value=f"{cfg.get(key, 0.0):.2f}")
        tk.Entry(card, textvariable=self._var, width=10,
                 bg=PANEL_BG, fg=TEXT, insertbackground=TEXT,
                 font=FT, relief="flat").grid(row=1, column=1, padx=4, pady=4)

        def _save():
            try:
                val = float(self._var.get())
                c = _load_config()
                c[key] = val
                _save_config(c)
                self.log(f"M{self.motor_id}: Default position set to {val:.2f} deg")
                self.destroy()
            except ValueError:
                self.log(f"M{self.motor_id}: Invalid value — enter a number")

        _btn(card, "SAVE", _save, fg=GREEN, width=8).grid(row=1, column=2, padx=(8,0), pady=4)


class ParamPanel(tk.Toplevel):
    """
    Modal popup with editable read/write boxes for all motor parameters.

    Layout per PID row:
      [Parameter name]  [Current value (green)]  [New value entry]  [Write RAM]  [Write ROM]

    Bulk Read All / Write All buttons at the bottom of each section.
    Acceleration section below PID.
    """

    def __init__(self, parent, motor_id: int, can_mgr: CANManager,
                 log_cb: Callable, accent: str):
        super().__init__(parent)
        self.motor_id = motor_id
        self.can_mgr  = can_mgr
        self.log      = log_cb
        self.accent   = accent

        self.title(f"Motor {motor_id}  -  Parameters")
        self.configure(bg=BG)
        self.resizable(False, False)
        self.grab_set()
        self._build()

        self.update_idletasks()
        px = parent.winfo_rootx()
        py = parent.winfo_rooty()
        self.geometry(f"+{px + 40}+{py + 40}")

    # ------------------------------------------------------------------
    def _build(self):
        # ── Title ──────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg=BG)
        hdr.pack(fill="x", padx=16, pady=(14, 6))
        tk.Label(hdr, text=f"MOTOR {self.motor_id}  -  PARAMETERS",
                 fg=self.accent, bg=BG, font=FT_HEAD).pack(side="left")

        # ── PID card ───────────────────────────────────────────────────
        card = tk.Frame(self, bg=PANEL_BG, padx=16, pady=12)
        card.pack(fill="x", padx=16, pady=(0, 6))

        tk.Label(card, text="PID GAINS  (each register: 0 - 255)",
                 fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).grid(
                     row=0, column=0, columnspan=5, sticky="w", pady=(0, 8))

        # column headers
        col_spec = [("PARAMETER", 18), ("CURRENT", 10), ("NEW VALUE", 10),
                    ("WRITE RAM", 10), ("WRITE ROM", 10)]
        for c, (h, w) in enumerate(col_spec):
            tk.Label(card, text=h, fg=TEXT_DIM, bg=PANEL_BG,
                     font=FT_S, width=w, anchor="w").grid(
                         row=1, column=c, padx=4, pady=2, sticky="w")

        self._pid_rows: dict = {}
        pid_fields = [
            ("current_kp",  "Current Loop  Kp"),
            ("current_ki",  "Current Loop  Ki"),
            ("speed_kp",    "Speed Loop    Kp"),
            ("speed_ki",    "Speed Loop    Ki"),
            ("position_kp", "Position Loop Kp"),
            ("position_ki", "Position Loop Ki"),
        ]

        for r, (field, label) in enumerate(pid_fields, start=2):
            cur_var = tk.StringVar(value="—")
            new_var = tk.StringVar(value="")

            tk.Label(card, text=label, fg=TEXT, bg=PANEL_BG,
                     font=FT, width=20, anchor="w").grid(row=r, column=0, padx=4, pady=3, sticky="w")

            tk.Label(card, textvariable=cur_var, fg=GREEN, bg=PANEL_BG,
                     font=FT, width=10, anchor="w").grid(row=r, column=1, padx=4, sticky="w")

            ent = _entry(card, new_var, width=10, fg=self.accent)
            ent.grid(row=r, column=2, padx=4)

            ram = _btn(card, "RAM", lambda f=field: self._write_field_ram(f),
                       fg=self.accent, width=9)
            ram.grid(row=r, column=3, padx=4, pady=2)

            rom = _btn(card, "ROM  !", lambda f=field: self._write_field_rom(f),
                       fg=WARN, width=9)
            rom.grid(row=r, column=4, padx=4, pady=2)

            self._pid_rows[field] = {"cur": cur_var, "new": new_var}

        # bulk buttons
        bulk = tk.Frame(card, bg=PANEL_BG)
        bulk.grid(row=len(pid_fields)+2, column=0, columnspan=5,
                  sticky="ew", pady=(12, 2))

        _btn(bulk, "READ ALL PID",       self._read_pid_all,      fg=self.accent, width=16).pack(side="left", padx=3)
        _btn(bulk, "WRITE ALL -> RAM",   self._write_pid_all_ram, fg=self.accent, width=17).pack(side="left", padx=3)
        _btn(bulk, "WRITE ALL -> ROM !",  self._write_pid_all_rom, fg=WARN,        width=18).pack(side="left", padx=3)

        # ── Default position card ──────────────────────────────────────
        dcard = tk.Frame(self, bg=PANEL_BG, padx=16, pady=12)
        dcard.pack(fill="x", padx=16, pady=(0, 6))
        tk.Label(dcard, text="DEFAULT POSITION  (degrees, absolute encoder)",
                 fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).grid(
                     row=0, column=0, columnspan=4, sticky="w", pady=(0, 8))

        cfg = _load_config()
        key = f"motor{self.motor_id}_default_pos"

        tk.Label(dcard, text="Go-to position on INIT", fg=TEXT, bg=PANEL_BG,
                 font=FT, width=24, anchor="w").grid(row=1, column=0, padx=4, pady=4, sticky="w")

        self._def_pos_var = tk.StringVar(value=f"{cfg.get(key, 0.0):.2f}")
        tk.Entry(dcard, textvariable=self._def_pos_var, width=10,
                 bg=PANEL_BG, fg=TEXT, insertbackground=TEXT,
                 font=FT, relief="flat").grid(row=1, column=1, padx=4, pady=4)

        def _save_default():
            try:
                val = float(self._def_pos_var.get())
                c = _load_config()
                c[key] = val
                _save_config(c)
                self.log(f"M{self.motor_id}: Default position set to {val:.2f} deg")
            except ValueError:
                self.log(f"M{self.motor_id}: Invalid default position value")

        _btn(dcard, "SAVE", _save_default, fg=GREEN, width=8).grid(
            row=1, column=2, padx=4, pady=4)

        # ── Acceleration card ──────────────────────────────────────────
        acard = tk.Frame(self, bg=PANEL_BG, padx=16, pady=12)
        acard.pack(fill="x", padx=16, pady=(0, 6))

        tk.Label(acard, text="ACCELERATION",
                 fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).grid(
                     row=0, column=0, columnspan=5, sticky="w", pady=(0, 8))

        for c, (h, w) in enumerate(col_spec):
            tk.Label(acard, text=h, fg=TEXT_DIM, bg=PANEL_BG,
                     font=FT_S, width=w, anchor="w").grid(
                         row=1, column=c, padx=4, pady=2, sticky="w")

        tk.Label(acard, text="Acceleration  (dps squared)", fg=TEXT, bg=PANEL_BG,
                 font=FT, width=20, anchor="w").grid(row=2, column=0, padx=4, pady=3, sticky="w")

        self._accel_cur = tk.StringVar(value="—")
        self._accel_new = tk.StringVar(value="")

        tk.Label(acard, textvariable=self._accel_cur, fg=GREEN, bg=PANEL_BG,
                 font=FT, width=10, anchor="w").grid(row=2, column=1, padx=4, sticky="w")

        _entry(acard, self._accel_new, width=10, fg=self.accent).grid(row=2, column=2, padx=4)

        _btn(acard, "RAM", self._write_accel_ram, fg=self.accent, width=9).grid(
            row=2, column=3, padx=4, pady=2)

        # no ROM command for acceleration in standard RMD protocol
        tk.Label(acard, text="(RAM only)", fg=TEXT_DIM, bg=PANEL_BG,
                 font=FT_S).grid(row=2, column=4, padx=4)

        bulk_a = tk.Frame(acard, bg=PANEL_BG)
        bulk_a.grid(row=3, column=0, columnspan=5, sticky="ew", pady=(10, 2))
        _btn(bulk_a, "READ ACCELERATION", self._read_accel, fg=self.accent, width=18).pack(side="left", padx=3)

        # ── ROM warning ────────────────────────────────────────────────
        wf = tk.Frame(self, bg=BG, padx=16, pady=8)
        wf.pack(fill="x")
        tk.Label(wf,
                 text="  !  ROM writes are permanent and flash has limited endurance (~100k cycles).\n"
                      "     Use RAM writes while tuning. Only write to ROM to persist final values.",
                 fg=WARN, bg=BG, font=FT_S, justify="left").pack(anchor="w")

        # ── Close ──────────────────────────────────────────────────────
        _btn(self, "CLOSE", self.destroy, fg=TEXT_DIM, width=12).pack(pady=(4, 14))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _motor(self):
        m = self.can_mgr.get_motor(self.motor_id)
        if m is None:
            messagebox.showerror("Not connected",
                                 "CAN bus is not connected or motor not initialized.",
                                 parent=self)
        return m

    def _bg(self, fn):
        threading.Thread(target=fn, daemon=True).start()

    def _populate_pid(self, p: PIDParams):
        for field in ("current_kp", "current_ki", "speed_kp",
                      "speed_ki", "position_kp", "position_ki"):
            self._pid_rows[field]["cur"].set(str(getattr(p, field)))

    def _read_new_values(self) -> Optional[PIDParams]:
        """Parse all 'new value' entries. Blank = keep current. Returns None on error."""
        vals = {}
        for field, row in self._pid_rows.items():
            raw = row["new"].get().strip()
            if raw == "":
                try:    vals[field] = int(row["cur"].get())
                except: vals[field] = 0
            else:
                try:
                    v = int(raw)
                    if not 0 <= v <= 255:
                        raise ValueError
                    vals[field] = v
                except ValueError:
                    messagebox.showerror("Invalid value",
                                         f"{field}: must be an integer 0-255  (got: '{raw}')",
                                         parent=self)
                    return None
        return PIDParams(**vals)

    # ------------------------------------------------------------------
    # PID read / write actions
    # ------------------------------------------------------------------

    def _read_pid_all(self):
        def work():
            m = self._motor()
            if m is None: return
            p = m.read_pid()
            if p:
                self.after(0, lambda: self._populate_pid(p))
                self.log(f"M{self.motor_id}: PID read  "
                         f"I(kp={p.current_kp} ki={p.current_ki})  "
                         f"V(kp={p.speed_kp} ki={p.speed_ki})  "
                         f"P(kp={p.position_kp} ki={p.position_ki})")
            else:
                self.log(f"M{self.motor_id}: PID read - no reply")
        self._bg(work)

    def _write_pid_all_ram(self):
        p = self._read_new_values()
        if p is None: return
        def work():
            m = self._motor()
            if m is None: return
            echo = m.write_pid_ram(p)
            if echo:
                self.after(0, lambda: self._populate_pid(echo))
                self.log(f"M{self.motor_id}: All PID -> RAM  "
                         f"I({echo.current_kp},{echo.current_ki})  "
                         f"V({echo.speed_kp},{echo.speed_ki})  "
                         f"P({echo.position_kp},{echo.position_ki})")
            else:
                self.log(f"M{self.motor_id}: PID RAM write - no reply")
        self._bg(work)

    def _write_pid_all_rom(self):
        p = self._read_new_values()
        if p is None: return
        if not messagebox.askyesno(
                "Write all PID to ROM",
                "This will permanently save ALL PID gains to motor ROM.\n\n"
                "ROM has limited write cycles. Continue?",
                parent=self):
            return
        def work():
            m = self._motor()
            if m is None: return
            echo = m.write_pid_rom(p)
            if echo:
                self.after(0, lambda: self._populate_pid(echo))
                self.log(f"M{self.motor_id}: All PID -> ROM  "
                         f"I({echo.current_kp},{echo.current_ki})  "
                         f"V({echo.speed_kp},{echo.speed_ki})  "
                         f"P({echo.position_kp},{echo.position_ki})")
            else:
                self.log(f"M{self.motor_id}: PID ROM write - no reply")
        self._bg(work)

    def _write_field_ram(self, field: str):
        """Write a single PID field to RAM, reading all others first."""
        raw = self._pid_rows[field]["new"].get().strip()
        if not raw:
            self.log(f"M{self.motor_id}: No value entered for {field}")
            return
        try:
            v = int(raw)
            assert 0 <= v <= 255
        except (ValueError, AssertionError):
            messagebox.showerror("Invalid value", f"{field} must be 0-255", parent=self)
            return

        def work():
            m = self._motor()
            if m is None: return
            current = m.read_pid()
            if current is None:
                self.log(f"M{self.motor_id}: Could not read PID before single-field write")
                return
            setattr(current, field, v)
            echo = m.write_pid_ram(current)
            if echo:
                self.after(0, lambda: self._populate_pid(echo))
                self.log(f"M{self.motor_id}: {field} = {v} -> RAM")
            else:
                self.log(f"M{self.motor_id}: {field} RAM write - no reply")
        self._bg(work)

    def _write_field_rom(self, field: str):
        """Write a single PID field to ROM."""
        raw = self._pid_rows[field]["new"].get().strip()
        if not raw:
            self.log(f"M{self.motor_id}: No value entered for {field}")
            return
        try:
            v = int(raw)
            assert 0 <= v <= 255
        except (ValueError, AssertionError):
            messagebox.showerror("Invalid value", f"{field} must be 0-255", parent=self)
            return
        if not messagebox.askyesno(
                "Write to ROM",
                f"Permanently save  {field} = {v}  to motor ROM?\n\n"
                "ROM has limited write cycles.",
                parent=self):
            return

        def work():
            m = self._motor()
            if m is None: return
            current = m.read_pid()
            if current is None:
                self.log(f"M{self.motor_id}: Could not read PID before single-field ROM write")
                return
            setattr(current, field, v)
            echo = m.write_pid_rom(current)
            if echo:
                self.after(0, lambda: self._populate_pid(echo))
                self.log(f"M{self.motor_id}: {field} = {v} -> ROM")
            else:
                self.log(f"M{self.motor_id}: {field} ROM write - no reply")
        self._bg(work)

    # ------------------------------------------------------------------
    # Acceleration actions
    # ------------------------------------------------------------------

    def _read_accel(self):
        def work():
            m = self._motor()
            if m is None: return
            a = m.read_acceleration()
            if a:
                self.after(0, lambda: self._accel_cur.set(str(a.acceleration_dps2)))
                self.log(f"M{self.motor_id}: Acceleration = {a.acceleration_dps2} dps^2")
            else:
                self.log(f"M{self.motor_id}: Acceleration read - no reply")
        self._bg(work)

    def _write_accel_ram(self):
        raw = self._accel_new.get().strip()
        if not raw:
            self.log(f"M{self.motor_id}: No acceleration value entered")
            return
        try:
            v = int(raw)
            assert v >= 1
        except (ValueError, AssertionError):
            messagebox.showerror("Invalid value", "Acceleration must be an integer >= 1", parent=self)
            return

        def work():
            m = self._motor()
            if m is None: return
            echo = m.write_acceleration_ram(v)
            if echo:
                self.after(0, lambda: self._accel_cur.set(str(echo.acceleration_dps2)))
                self.log(f"M{self.motor_id}: Acceleration = {echo.acceleration_dps2} dps^2 -> RAM")
            else:
                self.log(f"M{self.motor_id}: Acceleration write - no reply")
        self._bg(work)


# ============================================================
# Motor panel  (one per motor)
# ============================================================

class MotorPanel(tk.Frame):

    def __init__(self, parent, panel_idx: int, can_mgr: CANManager,
                 log_cb: Callable, **kw):
        super().__init__(parent, bg=PANEL_BG, **kw)
        self.motor_id     = panel_idx   # used as display label AND as CANManager lookup key
        self.can_mgr      = can_mgr
        self.log          = log_cb
        self._initialized = False
        self._homing      = False
        self._pos_offset  = 0.0
        self._sw_pos      = 0.0   # software-tracked position in raw motor degrees
        self._moving      = False # True while a jog/goto command is in progress
        self._stall_since = None  # monotonic time when high current first seen
        self._q: queue.Queue = queue.Queue()
        threading.Thread(target=self._worker, daemon=True).start()
        self.after(200, self._poll)
        self._build()

    def _build(self):
        self._ac = ACCENT if self.motor_id == 1 else ACCENT2

        # title row
        tr = tk.Frame(self, bg=PANEL_BG)
        tr.pack(fill="x", padx=14, pady=(14, 4))
        tk.Label(tr, text=f"MOTOR {self.motor_id}",
                 fg=self._ac, bg=PANEL_BG, font=FT_TITLE).pack(side="left")
        self._dot = tk.Label(tr, text="●", fg=RED, bg=PANEL_BG, font=("Calibri", 12))
        self._dot.pack(side="right")
        self._slbl = tk.Label(tr, text="NOT INITIALIZED",
                              fg=TEXT_DIM, bg=PANEL_BG, font=FT_S)
        self._slbl.pack(side="right", padx=6)

        _sep(self).pack(fill="x", padx=14, pady=4)

        # position readout
        pf = tk.Frame(self, bg=PANEL_BG)
        pf.pack(fill="x", padx=14, pady=4)
        _lbl(pf, "POSITION", TEXT_DIM, FT_S).pack(anchor="w")
        pr = tk.Frame(pf, bg=PANEL_BG)
        pr.pack(fill="x")
        self._pos_v = tk.StringVar(value="—")
        tk.Label(pr, textvariable=self._pos_v, fg=GREEN, bg=PANEL_BG, font=FT_BIG).pack(side="left")
        tk.Label(pr, text=" deg", fg=TEXT_DIM, bg=PANEL_BG, font=FT_MONO).pack(side="left", pady=4)

        # stats
        sf = tk.Frame(self, bg=PANEL_BG)
        sf.pack(fill="x", padx=14, pady=(0, 4))
        self._spd_v  = tk.StringVar(value="0 dps")
        self._cur_v  = tk.StringVar(value="0.00 A")
        self._tmp_v  = tk.StringVar(value="-- C")
        self._vlt_v  = tk.StringVar(value="-- V")
        for var, lbl in [(self._spd_v,"SPD"), (self._cur_v,"IQ"),
                          (self._tmp_v,"TMP"), (self._vlt_v,"VOLT")]:
            c = tk.Frame(sf, bg=PANEL_BG)
            c.pack(side="left", expand=True, fill="x")
            tk.Label(c, text=lbl, fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).pack()
            tk.Label(c, textvariable=var, fg=TEXT, bg=PANEL_BG, font=FT).pack()

        _sep(self).pack(fill="x", padx=14, pady=6)

        # control buttons
        cf = tk.Frame(self, bg=PANEL_BG)
        cf.pack(fill="x", padx=14, pady=4)

        self._init_btn = _btn(cf, f"INIT MOTOR {self.motor_id}", self._init, fg=self._ac)
        self._init_btn.pack(fill="x", pady=3)

        self._home_btn = _btn(cf, f"HOME MOTOR {self.motor_id}", self._home,
                              fg=YELLOW, state="disabled")
        self._home_btn.pack(fill="x", pady=3)

        self._param_btn = _btn(cf, "PARAMETERS  >", self._open_params,
                               fg=self._ac, state="disabled")
        self._param_btn.pack(fill="x", pady=3)

        self._settings_btn = _btn(cf, "SETTINGS  >", self._open_settings, fg=TEXT_DIM)
        self._settings_btn.pack(fill="x", pady=3)

        _sep(self).pack(fill="x", padx=14, pady=6)

        # jog
        _lbl(self, "JOG", TEXT_DIM, FT_S).pack(anchor="w", padx=14)
        sr = tk.Frame(self, bg=PANEL_BG)
        sr.pack(fill="x", padx=14, pady=4)
        _lbl(sr, "Step  deg:", TEXT).pack(side="left")
        self._step_v = tk.StringVar(value="1.0")
        _entry(sr, self._step_v, width=8, fg=self._ac).pack(side="left", padx=6)

        jr = tk.Frame(self, bg=PANEL_BG)
        jr.pack(fill="x", padx=14, pady=3)
        self._jup = _btn(jr, "  JOG UP  (+)", self._jog_up,   fg=self._ac, width=14, state="disabled")
        self._jup.pack(side="left", expand=True, fill="x", padx=(0, 4))
        self._jdn = _btn(jr, "  JOG DOWN (-)", self._jog_down, fg=self._ac, width=14, state="disabled")
        self._jdn.pack(side="left", expand=True, fill="x")

        _sep(self).pack(fill="x", padx=14, pady=6)

        # go to
        _lbl(self, "GO TO POSITION", TEXT_DIM, FT_S).pack(anchor="w", padx=14)
        gr = tk.Frame(self, bg=PANEL_BG)
        gr.pack(fill="x", padx=14, pady=4)
        self._goto_v = tk.StringVar(value="0.0")
        _entry(gr, self._goto_v, width=9, fg=self._ac).pack(side="left")
        _lbl(gr, "  deg", TEXT_DIM).pack(side="left")
        self._goto_btn = _btn(gr, "GO", self._goto, fg=self._ac, width=6, state="disabled")
        self._goto_btn.pack(side="left", padx=8)

        _sep(self).pack(fill="x", padx=14, pady=6)

        # stop
        self._stop_btn = _btn(self, "  STOP", self._stop, fg=RED, width=20, state="disabled")
        self._stop_btn.pack(fill="x", padx=14, pady=(0, 14))

    # ------------------------------------------------------------------
    def _setstatus(self, txt, fg=TEXT_DIM, dot=RED):
        self._slbl.config(text=txt, fg=fg)
        self._dot.config(fg=dot)

    def _enable(self, on: bool):
        s = "normal" if on else "disabled"
        for w in (self._home_btn, self._param_btn, self._jup,
                  self._jdn, self._goto_btn, self._stop_btn):
            w.config(state=s)

    def _motor(self): return self.can_mgr.get_motor(self.motor_id)

    def _step(self):
        try:    return float(self._step_v.get())
        except: return 1.0

    def _worker(self):
        while True:
            fn = self._q.get()
            try:    fn()
            except Exception as e: self.log(f"M{self.motor_id} ERR: {e}")
            finally: self._q.task_done()

    def _sub(self, fn): self._q.put(fn)

    def _poll(self):
        if self._initialized and not self._homing:
            self._sub(self._do_poll)
        self.after(500, self._poll)

    # Stall detection thresholds (can be tuned)
    STALL_AMPS   = 0.40   # current above this triggers stall check
    STALL_WINDOW = 0.3    # seconds current must stay high to confirm stall

    def _do_poll(self):
        m = self._motor()
        if m is None: return
        r = m._send_recv(bytes([CMD.READ_MOTOR_STATUS_2, 0,0,0,0,0,0,0]))
        if r and len(r.data) >= 8 and bytes(r.data)[0] == CMD.READ_MOTOR_STATUS_2:
            d = bytes(r.data)
            current = abs(struct.unpack_from("<h", d, 2)[0] / 100.0)
            speed   = float(struct.unpack_from("<h", d, 4)[0])
            temp    = d[1]

            # Stall detection — only while a move is in progress, not during homing
            if self._moving and not self._homing:
                if current >= self.STALL_AMPS:
                    if self._stall_since is None:
                        self._stall_since = time.monotonic()
                    elif time.monotonic() - self._stall_since >= self.STALL_WINDOW:
                        # Confirmed stall — stop motor
                        self._moving = False
                        self._stall_since = None
                        try: m.motor_stop()
                        except Exception: pass
                        self.after(0, lambda: [
                            self._setstatus("STALL DETECTED", RED, RED),
                            self.log(f"M{self.motor_id}: STALL — motor stopped  I={current:.2f}A"),
                        ])
                        return
                else:
                    self._stall_since = None  # current dropped, reset timer

            self.after(0, lambda sp=speed, c=current, t=temp: [
                self._spd_v.set(f"{sp:+.0f} dps"),
                self._cur_v.set(f"{c:.2f} A"),
                self._tmp_v.set(f"{t} C"),
                self._pos_v.set(f"{self._sw_pos - self._pos_offset:+.2f}"),
            ])

    # ------------------------------------------------------------------
    def _init(self): self._sub(self._do_init)

    def _do_init(self):
        self.after(0, lambda: [self._setstatus("Initializing...", YELLOW, YELLOW),
                               self._init_btn.config(state="disabled")])
        m = self._motor()
        if m is None:
            self.log(f"M{self.motor_id}: CAN not connected - use CONNECT CAN BUS first")
            self.after(0, lambda: [self._setstatus("CAN NOT CONNECTED", RED, RED),
                                   self._init_btn.config(state="normal")])
            return
        # Retry init a few times - motor may need a moment after power-on
        st = None
        for _ in range(5):
            st = m.read_status()
            if st is not None:
                break
            time.sleep(0.1)
        if st is None:
            self.log(f"M{self.motor_id}: No reply - check wiring / motor ID / power cycle motor")
            self.after(0, lambda: [self._setstatus("NO REPLY", RED, RED),
                                   self._init_btn.config(state="normal")])
            return
        # Read encoder to establish current position in motor frame
        enc = 0.0
        for _ in range(5):
            r2 = m._send_recv(bytes([CMD.READ_MOTOR_STATUS_2, 0,0,0,0,0,0,0]))
            if r2 and len(r2.data) >= 8 and bytes(r2.data)[0] == CMD.READ_MOTOR_STATUS_2:
                enc = struct.unpack_from("<H", bytes(r2.data), 6)[0] / 100.0
                break
            time.sleep(0.05)
        self._sw_pos     = enc
        self._pos_offset = 0.0
        if m:
            m.reset_unwrap()
        self._initialized = True
        self.log(f"M{self.motor_id}: Initialized  T={st.temperature}C  V={st.voltage:.1f}V  enc={enc:.2f} deg")

        # Go to default position
        cfg = _load_config()
        key = f"motor{self.motor_id}_default_pos"
        default_pos = float(cfg.get(key, 0.0))
        self.log(f"M{self.motor_id}: Moving to default position {default_pos:.2f} deg")
        self._sw_pos = default_pos
        m._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_1,
                            max(0, min(65535, int(360))),
                            int(default_pos * 100)))
        self.after(0, lambda p=default_pos: self._pos_v.set(f"{p:.2f}"))
        self.after(0, lambda: [self._setstatus("READY", GREEN, GREEN),
                               self._enable(True),
                               self._init_btn.config(state="normal")])

    # ------------------------------------------------------------------
    def _home(self):
        if not self._homing: self._sub(self._do_home)

    def _do_home(self):
        m = self._motor()
        if m is None: return
        self._homing = True
        self._home_cancel = False
        self.after(0, lambda: [self._enable(False),
                               self._stop_btn.config(state="normal"),
                               self._setstatus("HOMING...", YELLOW, YELLOW),
                               self._pos_v.set("—")])
        try:
            t0          = time.monotonic()
            peak        = 0.0
            last_log    = -9.0
            stall_start = None
            low_since   = None

            def prog(msg):
                el = time.monotonic() - t0
                self.log(f"M{self.motor_id}: [Homing {el:.1f}s] {msg}")
                self.after(0, lambda x=msg: self._setstatus(x[:26], YELLOW, YELLOW))

            def read_current() -> float:
                """Send a STATUS_2 request and read the reply."""
                for _ in range(3):
                    m._send(bytes([CMD.READ_MOTOR_STATUS_2, 0,0,0,0,0,0,0]))
                    deadline = time.monotonic() + 0.1
                    while time.monotonic() < deadline:
                        r = m.bus.recv(timeout=max(0.0, deadline - time.monotonic()))
                        if r and len(r.data) >= 8 and bytes(r.data)[0] == CMD.READ_MOTOR_STATUS_2:
                            return abs(struct.unpack_from("<h", bytes(r.data), 2)[0] / 100.0)
                return 0.0

            # ── Sync _sw_pos to motor's actual position before homing ───
            # Motor's internal counter may be anywhere. Read it via STATUS_2
            # single-turn, then sync by sending a position command to where
            # the motor already is — no motion, just establishes a reference.
            # We do this by reading single-turn, then sending motor_stop to
            # freeze it, then issuing a zero-distance position command.
            #
            # Better: use speed mode to home (smooth constant velocity),
            # but track position with a parallel counter that increments
            # at HOMING_SPEED_DPS. This gives us _sw_pos without position cmds.

            # Drain bus
            try:
                while m.bus.recv(timeout=0.0) is not None:
                    pass
            except Exception:
                pass
            time.sleep(0.1)

            # _sw_pos tracks position via time integration during speed-mode homing.
            # 0x92 is not supported by this firmware (always returns zero).
            # _sw_pos starts at its current value (0 after init, or last known pos).

            # ── Phase 1: speed mode, track position via elapsed time ────
            # Speed mode = smooth constant velocity, no position fighting.
            # Position tracking: _sw_pos += HOMING_SPEED_DPS * dt each loop.
            spd_cmd = struct.pack("<Bxxxi", CMD.SPEED_CONTROL,
                                  int(HOMING_SPEED_DPS * HOMING_DIRECTION * 100))

            prog("Approaching hard stop...")
            last_t = time.monotonic()

            while True:
                if self._home_cancel:
                    m.motor_stop()
                    prog("Cancelled")
                    return

                el = time.monotonic() - t0
                if el > HOMING_TIMEOUT_S:
                    m.motor_stop()
                    self.log(f"M{self.motor_id}: HOMING TIMEOUT")
                    self.after(0, lambda: self._setstatus("HOMING TIMEOUT", RED, RED))
                    return

                now = time.monotonic()
                dt  = now - last_t
                last_t = now

                # Advance position estimate by how far motor should have moved
                self._sw_pos += HOMING_SPEED_DPS * HOMING_DIRECTION * dt

                m._send(spd_cmd)
                ca = read_current()
                peak = max(peak, ca)
                self.after(0, lambda c=ca: self._cur_v.set(f"{c:.2f} A"))

                if el - last_log >= 1.0:
                    prog(f"I={ca:.2f}A  peak={peak:.2f}A  sw={self._sw_pos:.1f}")
                    last_log = el

                if ca >= HOMING_STALL_AMPS:
                    low_since = None
                    if stall_start is None:
                        stall_start = time.monotonic()
                    elif time.monotonic() - stall_start >= HOMING_STALL_WINDOW:
                        break
                else:
                    if stall_start is not None:
                        if low_since is None:
                            low_since = time.monotonic()
                        elif time.monotonic() - low_since > 0.5:
                            stall_start = None
                            low_since   = None
                    else:
                        low_since = None

            # ── Phase 2: stop, retract using INCREMENTAL position command ──
            # Use MOTOR_STOP only (not MOTOR_OFF/MOTOR_RUNNING which reset position ref)
            # Use POSITION_CONTROL_2 (0xA4) incremental command for retract —
            # it moves relative to current position, so absolute reference doesn't matter
            for _ in range(10):
                m._send(bytes([CMD.MOTOR_STOP, 0,0,0,0,0,0,0]))
            prog(f"Hard stop at sw={self._sw_pos:.2f}  peak={peak:.2f}A")
            time.sleep(0.5)
            # Drain bus
            try:
                while m.bus.recv(timeout=0.0) is not None:
                    pass
            except Exception:
                pass

            stall_sw = self._sw_pos
            # Incremental retract: positive = away from hard stop
            retract_inc = int(HOMING_RETRACT_DEG * (-HOMING_DIRECTION) * 100)
            retract_pos = stall_sw + HOMING_RETRACT_DEG * (-HOMING_DIRECTION)
            self._sw_pos = retract_pos

            prog(f"Retracting {HOMING_RETRACT_DEG} deg...")
            for _ in range(3):
                m._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_2,
                                    max(0, min(65535, int(HOMING_SPEED_DPS))),
                                    retract_inc))
                time.sleep(0.02)
            time.sleep(HOMING_RETRACT_DEG / max(HOMING_SPEED_DPS, 1) * 2 + 0.5)
            for _ in range(10):
                m._send(bytes([CMD.MOTOR_STOP, 0,0,0,0,0,0,0]))
            time.sleep(0.2)

            # ── Phase 3: zero ────────────────────────────────────────────
            # Motor is now at retract_pos in its old frame.
            # Reset its internal counter to 0 here so future position
            # commands start from 0 = home position.
            # motor_running() resets the counter — motor stays put physically.
            m._send(bytes([CMD.MOTOR_RUNNING, 0,0,0,0,0,0,0]))
            time.sleep(0.05)
            # Send position 0 — motor is already here, so no movement
            m._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_1,
                                max(0, min(65535, int(HOMING_SPEED_DPS))),
                                int(0)))
            time.sleep(0.2)
            # Now motor internal counter = 0, and our sw_pos = 0
            self._sw_pos     = 0.0
            self._pos_offset = 0.0
            el = time.monotonic() - t0
            self.log(f"M{self.motor_id}: Homing done {el:.1f}s  "
                     f"stall={stall_sw:.2f}  retract={retract_pos:.2f}  zeroed at 0")
            self.after(0, lambda: [self._setstatus("HOMED", GREEN, GREEN),
                                   self._pos_v.set("+0.00")])

        except Exception as e:
            self.log(f"M{self.motor_id}: Homing error — {e}")
            self.after(0, lambda: self._setstatus("HOMING ERROR", RED, RED))
        finally:
            self._homing = False
            self.after(0, lambda: self._enable(True))

    def _open_params(self):
        ParamPanel(self.winfo_toplevel(), self.motor_id,
                   self.can_mgr, self.log, self._ac)

    def _open_settings(self):
        SettingsPanel(self.winfo_toplevel(), self.motor_id, self.log)

    # ------------------------------------------------------------------
    def _jog_up(self):
        s = self._step(); self._sub(lambda: self._do_jog(+s))

    def _jog_down(self):
        s = self._step(); self._sub(lambda: self._do_jog(-s))

    def _do_jog(self, inc):
        m = self._motor()
        if m is None: return
        # _sw_pos tracks absolute motor-frame position.
        # _pos_offset is set to enc at init, so display = _sw_pos - _pos_offset.
        # raw_cmd = _sw_pos in 0.01 deg units sent to motor.
        self._sw_pos += inc
        raw_cmd = int(self._sw_pos * 100)
        display = self._sw_pos - self._pos_offset
        self.log(f"M{self.motor_id}: Jog {inc:+.2f} deg  -> {display:.2f}  raw={raw_cmd}")
        self._moving = True
        self._stall_since = None
        m._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_1,
                            max(0, min(65535, int(360))),
                            raw_cmd))
        self.after(0, lambda p=display: self._pos_v.set(f"{p:+.2f}"))

    def _goto(self):
        try:    tgt = float(self._goto_v.get())
        except: self.log(f"M{self.motor_id}: Invalid target"); return
        self._sub(lambda t=tgt: self._do_goto(t))

    def _do_goto(self, tgt):
        m = self._motor()
        if m is None: return
        # tgt is in display units — convert to motor frame
        raw = tgt + self._pos_offset
        self._sw_pos = raw
        self.log(f"M{self.motor_id}: Go to {tgt:.2f} deg  (raw={raw:.2f})")
        self._moving = True
        self._stall_since = None
        m._send(struct.pack("<BxHi", CMD.POSITION_CONTROL_1,
                            max(0, min(65535, int(360))),
                            int(raw * 100)))
        self.after(0, lambda p=tgt: self._pos_v.set(f"{p:+.2f}"))

    def _stop(self):
        self._home_cancel = True          # checked each homing loop iteration
        self._moving      = False
        self._stall_since = None
        m = self._motor()
        if m:
            try: m.motor_stop()           # send stop directly, bypass queue
            except Exception: pass
        self.log(f"M{self.motor_id}: Stopped")


# ============================================================
# Main application
# ============================================================

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RMD-L-4015  |  Dual Motor Controller")
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.configure(bg=BG)
        self.resizable(False, False)
        self.can_mgr = CANManager()
        self._build()
        self.protocol("WM_DELETE_WINDOW", self._close)
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{(self.winfo_screenwidth()-w)//2}+{(self.winfo_screenheight()-h)//2}")

    def _build(self):
        # header
        h = tk.Frame(self, bg=BG)
        h.pack(fill="x", padx=18, pady=(16, 8))
        tk.Label(h, text="RMD-L-4015  DUAL MOTOR CONTROLLER",
                 fg=ACCENT, bg=BG, font=FT_HEAD).pack(side="left")
        tk.Label(h, text="MyActuator  //  CAN Bus",
                 fg=TEXT_DIM, bg=BG, font=FT_S).pack(side="right")

        # CAN bar
        cb = tk.Frame(self, bg=PANEL_BG, padx=14, pady=8)
        cb.pack(fill="x", padx=18, pady=(0, 10))
        tk.Label(cb, text="CAN BUS", fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).pack(side="left")
        self._can_lbl = tk.Label(cb, text="DISCONNECTED", fg=RED, bg=PANEL_BG, font=FT_S)
        self._can_lbl.pack(side="left", padx=10)
        tk.Label(cb,
                 text=f"{CAN_INTERFACE}  |  Motor 1 → ch {MOTOR_1_CHANNEL}   Motor 2 → ch {MOTOR_2_CHANNEL}   @ {CAN_BITRATE//1000}k",
                 fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).pack(side="left", padx=6)
        self._conn_btn = _btn(cb, "CONNECT CAN BUS", self._connect, fg=ACCENT, width=16)
        self._conn_btn.pack(side="right")
        self._disc_btn = _btn(cb, "DISCONNECT", self._disconnect, fg=RED, width=10)
        self._disc_btn.pack(side="right", padx=(0, 6))
        self._disc_btn.config(state="disabled")

        # panels — use panel index 1 / 2 as the lookup key (not CAN node ID)
        row = tk.Frame(self, bg=BG)
        row.pack(padx=18)
        self.p1 = MotorPanel(row, panel_idx=1, can_mgr=self.can_mgr, log_cb=self._log)
        self.p1.pack(side="left", padx=(0, 8), fill="both")
        tk.Frame(row, bg=BORDER, width=2).pack(side="left", fill="y", pady=10)
        self.p2 = MotorPanel(row, panel_idx=2, can_mgr=self.can_mgr, log_cb=self._log)
        self.p2.pack(side="left", padx=(8, 0), fill="both")

        # log
        lf = tk.Frame(self, bg=BG)
        lf.pack(fill="x", padx=18, pady=(8, 16))
        tk.Label(lf, text="LOG", fg=TEXT_DIM, bg=BG, font=FT_S).pack(anchor="w")
        inn = tk.Frame(lf, bg=PANEL_BG)
        inn.pack(fill="x")
        self._log_box = tk.Text(inn, height=7, bg=PANEL_BG, fg=TEXT,
                                font=("Calibri", 8), relief="flat", bd=0,
                                state="disabled", wrap="word",
                                insertbackground=ACCENT)
        sc = tk.Scrollbar(inn, orient="vertical", command=self._log_box.yview,
                          bg=PANEL_BG, troughcolor=PANEL_BG, relief="flat")
        self._log_box.config(yscrollcommand=sc.set)
        sc.pack(side="right", fill="y")
        self._log_box.pack(side="left", fill="both", expand=True, padx=6, pady=4)
        for tag, col in [("green", GREEN), ("red", RED), ("yellow", YELLOW),
                          ("warn", WARN), ("dim", TEXT_DIM)]:
            self._log_box.tag_configure(tag, foreground=col)
        self._log("System ready.  Click CONNECT CAN BUS to begin.", "dim")

    def _on_close(self):
        """Clean up CAN buses before closing."""
        try:
            with self.can_mgr._lock:
                self.can_mgr._close_all()
        except Exception:
            pass
        self.destroy()

    def _connect(self):
        self._conn_btn.config(state="disabled")
        self._can_lbl.config(text="Connecting...", fg=YELLOW)

        def work():
            ok, msg = self.can_mgr.connect()
            if ok:
                self.after(0, lambda: self._can_lbl.config(text="CONNECTED", fg=GREEN))
                self.after(0, lambda m=msg: self._log(m, "green"))
                self.after(0, lambda: self._disc_btn.config(state="normal"))
            else:
                self.after(0, lambda: self._can_lbl.config(text="FAILED", fg=RED))
                self.after(0, lambda m=msg: self._log(m, "red"))
                self.after(0, lambda: self._conn_btn.config(state="normal"))

        threading.Thread(target=work, daemon=True).start()

    def _disconnect(self):
        """Stop motors and mark as disconnected — keeps USB layer alive for reconnect."""
        self._disc_btn.config(state="disabled")
        self._conn_btn.config(state="disabled")
        self._can_lbl.config(text="Disconnecting...", fg=YELLOW)

        def work():
            import time as _t
            try:
                with self.can_mgr._lock:
                    for driver in self.can_mgr._drivers.values():
                        try:
                            driver.motor_stop()
                            _t.sleep(0.05)
                            driver.motor_off()
                        except Exception: pass
                    # Shutdown buses cleanly — connect() will reopen them fresh
                    self.can_mgr._close_all()
            except Exception as e:
                self._log(f"Disconnect error: {e}", "red")
            self.after(0, lambda: [
                self._can_lbl.config(text="DISCONNECTED", fg=RED),
                self._conn_btn.config(state="normal"),
                self._disc_btn.config(state="disabled"),
            ])
            self._log("Disconnected. Click CONNECT CAN BUS to reconnect.", "dim")

        threading.Thread(target=work, daemon=True).start()

    def _log(self, text: str, tag: str = ""):
        ts = time.strftime("%H:%M:%S")
        line = f"[{ts}] {text}\n"
        def ins():
            self._log_box.config(state="normal")
            self._log_box.insert("end", line, tag)
            self._log_box.see("end")
            self._log_box.config(state="disabled")
        self.after(0, ins)

    def _close(self):
        self.can_mgr.shutdown()
        self.destroy()


# ============================================================
if __name__ == "__main__":
    if not CAN_AVAILABLE:
        _r = tk.Tk(); _r.withdraw()
        messagebox.showwarning(
            "python-can not found",
            "python-can is not installed.\n\n"
            "The UI will open in preview mode (no motor commands).\n\n"
            "Install with:  pip install python-can",
        )
        _r.destroy()
    App().mainloop()
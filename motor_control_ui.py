"""
MyActuator RMD-L-4015  -  Dual Motor Control Panel
====================================================
Windows desktop GUI built with tkinter (stdlib, no extra install).
Motor communication uses python-can.

Requirements
------------
    pip install python-can

CAN adapter configuration
--------------------------
Edit the three constants below to match your hardware.

  Adapter          CAN_INTERFACE   CAN_CHANNEL
  ---------------------------------------------
  PEAK PCAN-USB    pcan            PCAN_USBBUS1
  KVASER           kvaser          0
  SLCAN / Canable  slcan           COM3
  Vector           vector          0
"""

# ============================================================
# CAN adapter
# ============================================================
CAN_INTERFACE = "pcan"
CAN_CHANNEL   = "PCAN_USBBUS1"
CAN_BITRATE   = 1_000_000

MOTOR_1_ID = 1
MOTOR_2_ID = 2

# Homing defaults
HOMING_SPEED_DPS    = 60.0
HOMING_STALL_AMPS   = 1.5
HOMING_STALL_WINDOW = 0.15
HOMING_TIMEOUT_S    = 20.0
HOMING_RETRACT_DEG  = 5.0
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
    POSITION_CONTROL_1     = 0xA3
    POSITION_CONTROL_4     = 0xA6
    WRITE_CURRENT_POS_ZERO = 0x19
    READ_MULTI_TURN_ANGLE  = 0x92
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

    def __init__(self, motor_id: int, bus, timeout: float = 0.15):
        self.motor_id = motor_id
        self.can_id   = self.BASE_ID  + motor_id
        self.reply_id = self.REPLY_ID + motor_id
        self.bus      = bus
        self.timeout  = timeout

    def _send(self, data: bytes):
        msg = can.Message(arbitration_id=self.can_id, data=data, is_extended_id=False)
        self.bus.send(msg)

    def _send_recv(self, data: bytes):
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

    def set_speed(self, dps: float):
        spd = max(-2_147_483_648, min(2_147_483_647, int(dps * 100)))
        r = self._send_recv(struct.pack("<Bxxxi", CMD.SPEED_CONTROL, spd))
        return self._parse_s2(r) if r else None

    def set_position_multiturn(self, deg: float, max_dps: float = 360.0):
        r = self._send_recv(struct.pack("<BxHi", CMD.POSITION_CONTROL_1,
                                        max(0, min(65535, int(max_dps))),
                                        int(deg * 100)))
        return self._parse_s2(r) if r else None

    def set_position_increment(self, inc: float, max_dps: float = 360.0):
        r = self._send_recv(struct.pack("<BxHi", CMD.POSITION_CONTROL_4,
                                        max(0, min(65535, int(max_dps))),
                                        int(inc * 100)))
        return self._parse_s2(r) if r else None

    def read_status(self):
        r = self._send_recv(bytes([CMD.READ_MOTOR_STATUS_2, 0,0,0,0,0,0,0]))
        return self._parse_s2(r) if r else None

    def read_status_1(self):
        r = self._send_recv(bytes([CMD.READ_MOTOR_STATUS_1, 0,0,0,0,0,0,0]))
        if r is None: return None
        d = r.data
        return MotorStatus(temperature=d[1],
                           voltage=struct.unpack_from("<H", d, 3)[0] / 10.0,
                           error_state=struct.unpack_from("<H", d, 6)[0])

    def read_multi_turn_angle(self):
        r = self._send_recv(bytes([CMD.READ_MULTI_TURN_ANGLE, 0,0,0,0,0,0,0]))
        return struct.unpack_from("<q", r.data, 1)[0] / 100.0 if r else None

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
    def __init__(self):
        self.bus = None
        self.motors: dict = {}

    def connect(self):
        if not CAN_AVAILABLE:
            return False, "python-can not installed.\nRun: pip install python-can"
        try:
            if self.bus:
                self.bus.shutdown()
            self.bus = can.Bus(interface=CAN_INTERFACE, channel=CAN_CHANNEL, bitrate=CAN_BITRATE)
            self.motors[MOTOR_1_ID] = MotorDriver(MOTOR_1_ID, self.bus)
            self.motors[MOTOR_2_ID] = MotorDriver(MOTOR_2_ID, self.bus)
            return True, "CAN bus connected."
        except Exception as e:
            return False, f"CAN connection failed:\n{e}"

    def get_motor(self, mid: int):
        return self.motors.get(mid)

    def shutdown(self):
        for m in self.motors.values():
            try: m.motor_off()
            except: pass
        if self.bus:
            try: self.bus.shutdown()
            except: pass


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

FT        = ("Consolas",  9)
FT_S      = ("Consolas",  8)
FT_BIG    = ("Consolas", 22, "bold")
FT_MONO   = ("Consolas", 14, "bold")
FT_HEAD   = ("Consolas", 11, "bold")
FT_TITLE  = ("Consolas", 13, "bold")


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

    def __init__(self, parent, motor_id: int, can_mgr: CANManager,
                 log_cb: Callable, **kw):
        super().__init__(parent, bg=PANEL_BG, **kw)
        self.motor_id     = motor_id
        self.can_mgr      = can_mgr
        self.log          = log_cb
        self._initialized = False
        self._homing      = False
        self._pos_offset  = 0.0
        self._q: queue.Queue = queue.Queue()
        threading.Thread(target=self._worker, daemon=True).start()
        self.after(500, self._poll)
        self._build()

    def _build(self):
        self._ac = ACCENT if self.motor_id == 1 else ACCENT2

        # title row
        tr = tk.Frame(self, bg=PANEL_BG)
        tr.pack(fill="x", padx=14, pady=(14, 4))
        tk.Label(tr, text=f"MOTOR {self.motor_id}",
                 fg=self._ac, bg=PANEL_BG, font=FT_TITLE).pack(side="left")
        self._dot = tk.Label(tr, text="●", fg=RED, bg=PANEL_BG, font=("Consolas", 12))
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

    def _do_poll(self):
        m = self._motor()
        if m is None: return
        ang = m.read_multi_turn_angle()
        st  = m.read_status()
        s1  = m.read_status_1()
        if ang is not None:
            d = ang - self._pos_offset
            self.after(0, lambda a=d: self._pos_v.set(f"{a:+.2f}"))
        if st:
            self.after(0, lambda s=st: [
                self._spd_v.set(f"{s.speed:+.0f} dps"),
                self._cur_v.set(f"{s.current:.2f} A"),
                self._tmp_v.set(f"{s.temperature} C"),
            ])
        if s1:
            self.after(0, lambda v=s1.voltage: self._vlt_v.set(f"{v:.1f} V"))

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
        st = m.read_status()
        if st is None:
            self.log(f"M{self.motor_id}: No reply - check wiring / motor ID")
            self.after(0, lambda: [self._setstatus("NO REPLY", RED, RED),
                                   self._init_btn.config(state="normal")])
            return
        self._initialized = True
        self.log(f"M{self.motor_id}: Initialized  T={st.temperature}C  I={st.current:.2f}A")
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
        self.after(0, lambda: [self._enable(False),
                               self._setstatus("HOMING...", YELLOW, YELLOW),
                               self._pos_v.set("—")])

        def prog(msg, t):
            self.log(f"M{self.motor_id}: [Homing {t:.1f}s] {msg}")
            self.after(0, lambda x=msg: self._setstatus(x[:26], YELLOW, YELLOW))

        try:
            spd = HOMING_SPEED_DPS * HOMING_DIRECTION
            t0 = time.monotonic()
            ss = None
            peak = 0.0
            prog("Approaching hard stop...", 0)

            while True:
                el = time.monotonic() - t0
                if el > HOMING_TIMEOUT_S:
                    m.motor_stop()
                    self.log(f"M{self.motor_id}: HOMING TIMEOUT")
                    self.after(0, lambda: self._setstatus("HOMING TIMEOUT", RED, RED))
                    return
                st = m.set_speed(spd)
                if st is None:
                    time.sleep(0.02); continue
                ca = abs(st.current)
                peak = max(peak, ca)
                if ca >= HOMING_STALL_AMPS:
                    if ss is None:
                        ss = time.monotonic()
                        prog(f"Stall at {ca:.2f}A confirming...", el)
                    elif time.monotonic() - ss >= HOMING_STALL_WINDOW:
                        break
                else:
                    ss = None
                time.sleep(0.02)

            el = time.monotonic() - t0
            prog(f"Stall confirmed  I={peak:.2f}A", el)
            m.motor_stop(); time.sleep(0.15)

            ret = HOMING_RETRACT_DEG * (-HOMING_DIRECTION)
            prog(f"Retracting {HOMING_RETRACT_DEG:.1f} deg...", el)
            m.set_position_increment(ret, max_dps=HOMING_SPEED_DPS)
            time.sleep(HOMING_RETRACT_DEG / max(HOMING_SPEED_DPS, 1) + 0.6)
            m.motor_stop(); time.sleep(0.15)

            raw = m.read_multi_turn_angle() or 0.0
            self._pos_offset = raw
            el = time.monotonic() - t0
            prog("Home set.", el)
            self.log(f"M{self.motor_id}: Homing done in {el:.1f}s  offset={raw:.2f} deg")
            self.after(0, lambda: [self._setstatus("HOMED", GREEN, GREEN),
                                   self._pos_v.set("0.00")])
        except Exception as e:
            self.log(f"M{self.motor_id}: Homing error - {e}")
            self.after(0, lambda: self._setstatus("HOMING ERROR", RED, RED))
        finally:
            self._homing = False
            self.after(0, lambda: self._enable(True))

    # ------------------------------------------------------------------
    def _open_params(self):
        ParamPanel(self.winfo_toplevel(), self.motor_id,
                   self.can_mgr, self.log, self._ac)

    # ------------------------------------------------------------------
    def _jog_up(self):
        s = self._step(); self._sub(lambda: self._do_jog(+s))

    def _jog_down(self):
        s = self._step(); self._sub(lambda: self._do_jog(-s))

    def _do_jog(self, inc):
        m = self._motor()
        if m:
            self.log(f"M{self.motor_id}: Jog {inc:+.2f} deg")
            m.set_position_increment(inc, max_dps=180.0)

    def _goto(self):
        try:    tgt = float(self._goto_v.get())
        except: self.log(f"M{self.motor_id}: Invalid target"); return
        self._sub(lambda t=tgt: self._do_goto(t))

    def _do_goto(self, tgt):
        m = self._motor()
        if m:
            raw = tgt + self._pos_offset
            self.log(f"M{self.motor_id}: Go to {tgt:.2f} deg  (raw {raw:.2f} deg)")
            m.set_position_multiturn(raw, max_dps=360.0)

    def _stop(self):
        m = self._motor()
        if m:
            m.motor_stop()
            self.log(f"M{self.motor_id}: Stopped")


# ============================================================
# Main application
# ============================================================

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RMD-L-4015  |  Dual Motor Controller")
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
        tk.Label(cb, text=f"{CAN_INTERFACE} : {CAN_CHANNEL} @ {CAN_BITRATE//1000}k",
                 fg=TEXT_DIM, bg=PANEL_BG, font=FT_S).pack(side="left", padx=6)
        self._conn_btn = _btn(cb, "CONNECT CAN BUS", self._connect, fg=ACCENT, width=16)
        self._conn_btn.pack(side="right")

        # panels
        row = tk.Frame(self, bg=BG)
        row.pack(padx=18)
        self.p1 = MotorPanel(row, MOTOR_1_ID, self.can_mgr, self._log)
        self.p1.pack(side="left", padx=(0, 8), fill="both")
        tk.Frame(row, bg=BORDER, width=2).pack(side="left", fill="y", pady=10)
        self.p2 = MotorPanel(row, MOTOR_2_ID, self.can_mgr, self._log)
        self.p2.pack(side="left", padx=(8, 0), fill="both")

        # log
        lf = tk.Frame(self, bg=BG)
        lf.pack(fill="x", padx=18, pady=(8, 16))
        tk.Label(lf, text="LOG", fg=TEXT_DIM, bg=BG, font=FT_S).pack(anchor="w")
        inn = tk.Frame(lf, bg=PANEL_BG)
        inn.pack(fill="x")
        self._log_box = tk.Text(inn, height=7, bg=PANEL_BG, fg=TEXT,
                                font=("Consolas", 8), relief="flat", bd=0,
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

    def _connect(self):
        self._conn_btn.config(state="disabled")
        self._can_lbl.config(text="Connecting...", fg=YELLOW)

        def work():
            ok, msg = self.can_mgr.connect()
            if ok:
                self.after(0, lambda: self._can_lbl.config(text="CONNECTED", fg=GREEN))
                self.after(0, lambda: self._log(f"CAN bus connected ({CAN_INTERFACE}:{CAN_CHANNEL})", "green"))
            else:
                self.after(0, lambda: self._can_lbl.config(text="FAILED", fg=RED))
                self.after(0, lambda m=msg: self._log(m, "red"))
            self.after(0, lambda: self._conn_btn.config(state="normal"))

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

"""PTY-based LLDB session management.

Ported from /Users/bengamble/lldb-mcp/lldb_mcp.py with enhancements:
- Configurable command timeout (was hardcoded 10s)
- Logging via stderr instead of print
- Typed attributes
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import pty
import termios


class LldbSession:
    """Manages an interactive LLDB process via a pseudo-terminal."""

    def __init__(
        self,
        session_id: str,
        lldb_path: str = "lldb",
        working_dir: str | None = None,
        command_timeout: float = 30.0,
    ) -> None:
        self.id = session_id
        self.lldb_path = lldb_path
        self.working_dir = working_dir or os.getcwd()
        self.command_timeout = command_timeout
        self.process: asyncio.subprocess.Process | None = None
        self.master_fd: int | None = None
        self.slave_fd: int | None = None
        self.target: str | None = None
        self.ready: bool = False

    async def start(self) -> str:
        """Start the LLDB process with a PTY. Returns initial output."""
        self.master_fd, self.slave_fd = pty.openpty()

        # Disable echo on the terminal
        new_settings = termios.tcgetattr(self.slave_fd)
        new_settings[3] = new_settings[3] & ~termios.ECHO
        termios.tcsetattr(self.slave_fd, termios.TCSADRAIN, new_settings)

        # Start LLDB process
        self.process = await asyncio.create_subprocess_exec(
            self.lldb_path,
            stdin=self.slave_fd,
            stdout=self.slave_fd,
            stderr=self.slave_fd,
            cwd=self.working_dir,
        )

        # Close slave end in parent process
        os.close(self.slave_fd)
        self.slave_fd = None

        # Make master fd non-blocking
        flags = fcntl.fcntl(self.master_fd, fcntl.F_GETFL)
        fcntl.fcntl(self.master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        # Wait for initial prompt
        output = await self._read_until_prompt()
        self.ready = True

        # Verify LLDB is working
        version_output = await self.execute_command("version")
        return output + version_output

    async def execute_command(self, command: str) -> str:
        """Send a command to LLDB and return the output."""
        if not self.ready or not self.process:
            msg = "LLDB session is not ready"
            raise RuntimeError(msg)
        if self.process.returncode is not None:
            msg = f"LLDB process has terminated (code {self.process.returncode})"
            raise RuntimeError(msg)

        os.write(self.master_fd, f"{command}\n".encode())
        return await self._read_until_prompt()

    async def _read_until_prompt(self) -> str:
        """Read from LLDB until the (lldb) prompt appears or timeout."""
        if not self.master_fd:
            msg = "PTY not initialized"
            raise RuntimeError(msg)

        buffer = b""
        prompt = b"(lldb)"
        start = asyncio.get_event_loop().time()

        while True:
            elapsed = asyncio.get_event_loop().time() - start
            if elapsed > self.command_timeout:
                return buffer.decode("utf-8", errors="replace") + "\n[Timeout waiting for LLDB response]"

            if self.process and self.process.returncode is not None:
                if buffer:
                    return buffer.decode("utf-8", errors="replace")
                msg = f"LLDB process terminated (code {self.process.returncode})"
                raise RuntimeError(msg)

            try:
                chunk = os.read(self.master_fd, 4096)
                if chunk:
                    buffer += chunk
                    if prompt in buffer:
                        return buffer.decode("utf-8", errors="replace")
            except BlockingIOError:
                await asyncio.sleep(0.05)
            except OSError as e:
                if buffer:
                    return buffer.decode("utf-8", errors="replace") + f"\n[PTY error: {e}]"
                msg = f"PTY read error: {e}"
                raise RuntimeError(msg) from e

    async def cleanup(self) -> None:
        """Gracefully terminate the LLDB session."""
        try:
            if self.master_fd is not None:
                try:
                    os.write(self.master_fd, b"quit\n")
                    await asyncio.sleep(0.5)
                except OSError:
                    pass

            if self.process and self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), 2.0)
                except TimeoutError:
                    self.process.kill()
                    await self.process.wait()

            if self.master_fd is not None:
                os.close(self.master_fd)
                self.master_fd = None

            if self.slave_fd is not None:
                os.close(self.slave_fd)
                self.slave_fd = None

        except OSError:
            pass
        finally:
            self.process = None
            self.ready = False


async def check_lldb(lldb_path: str = "lldb") -> dict[str, str]:
    """Check if LLDB is installed and return version info."""
    try:
        proc = await asyncio.create_subprocess_exec(
            lldb_path,
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        version = stdout.decode("utf-8", errors="replace").strip()
        return {"installed": "true", "version": version, "path": lldb_path}
    except FileNotFoundError:
        return {"installed": "false", "version": "", "path": lldb_path, "error": f"lldb not found at '{lldb_path}'"}
    except Exception as e:
        return {"installed": "false", "version": "", "path": lldb_path, "error": str(e)}

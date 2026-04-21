'''
File description:
MPI bootstrap helpers used by the cleaner CLI and MPI execution modes.

This module deliberately hides the awkward parts of `mpi4py` startup and
shutdown. New users can read this file to understand why the project can still
import and run serial commands on systems where MPI is installed but the
process was not launched with `mpirun`.
'''

from __future__ import annotations

import os

class _DummyComm:
    """Fallback communicator used when mpi4py is unavailable."""
    def Get_rank(self) -> int:
        return 0
    def Get_size(self) -> int:
        return 1
    def gather(self, value, root=0):
        return [value]
    def barrier(self):
        return None

class _DummyMPI:
    """Fallback MPI namespace that mimics the tiny API this project needs."""
    COMM_WORLD = _DummyComm()

try:
    os.environ.setdefault("MPI4PY_RC_INITIALIZE", "0")
    import mpi4py
    mpi4py.rc.initialize = False
    from mpi4py import MPI as _REAL_MPI  # type: ignore
    MPI = _REAL_MPI
    MPI_AVAILABLE = True
except Exception:
    MPI = _DummyMPI()
    MPI_AVAILABLE = False


def ensure_mpi_initialized() -> None:
    """Initialize MPI lazily only when an MPI mode actually needs it."""
    if MPI_AVAILABLE and not MPI.Is_initialized():
        MPI.Init()


def finalize_mpi() -> None:
    """Finalize MPI if the current process owns an active MPI runtime."""
    if MPI_AVAILABLE and MPI.Is_initialized() and not MPI.Is_finalized():
        MPI.Finalize()

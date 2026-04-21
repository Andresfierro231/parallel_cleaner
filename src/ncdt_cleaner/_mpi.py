from __future__ import annotations

class _DummyComm:
    def Get_rank(self) -> int:
        return 0
    def Get_size(self) -> int:
        return 1
    def gather(self, value, root=0):
        return [value]
    def barrier(self):
        return None

class _DummyMPI:
    COMM_WORLD = _DummyComm()

try:
    from mpi4py import MPI as _REAL_MPI  # type: ignore
    MPI = _REAL_MPI
    MPI_AVAILABLE = True
except Exception:
    MPI = _DummyMPI()
    MPI_AVAILABLE = False

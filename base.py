from typing import (
    Optional,
    List,
    Tuple,
    Dict,
)
from abc import (
    ABC,
    abstractmethod,
)


class StoreClient(ABC):
    __slots__ = ()

    @classmethod
    @abstractmethod
    def query_range_count(
        cls,
        start_key: Optional[str] = None,
        stop_key: Optional[str] = None,
        start_inclusive: bool = True,
        stop_inclusive: bool = True,
    ) -> int:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def query_range(
        cls,
        start_key: Optional[str] = None,
        stop_key: Optional[str] = None,
        start_inclusive: bool = True,
        stop_inclusive: bool = True,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def query_range_raw(
        cls,
        start_key: Optional[str] = None,
        stop_key: Optional[str] = None,
        start_inclusive: bool = True,
        stop_inclusive: bool = True,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def add_keys(cls, keys: List[str]):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def remove_keys(cls, keys: List[str]):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def add_and_remove_keys(
        cls, keys_to_add: List[str], keys_to_remove: List[str]
    ):
        raise NotImplementedError


class HexaStore(ABC):
    @classmethod
    @abstractmethod
    def add_item(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def remove_item(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def lookup_items(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def count_items(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def update_item(cls, lookup: Dict[str, str], patch: Dict[str, str]):
        raise NotImplementedError


    @classmethod
    @abstractmethod
    def to_hexastore_key_set(cls, *args, **kwargs) -> List[str]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def hexastore_key_to_tuple(cls, key: str) -> Tuple[str, str, str]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_composite_key(cls, *args, **kwargs) -> str:
        raise NotImplementedError
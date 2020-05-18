import redis
from typing import List, Optional
import base


class StoreClient(base.StoreClient):

    r = redis.Redis(host="localhost", port=6379, db=0)

    NAMESPACE = "default"

    @classmethod
    def add_keys(cls, keys: List[str]):
        return cls.r.zadd(cls.NAMESPACE, mapping={key: 0 for key in keys})

    @classmethod
    def remove_keys(cls, keys: List[str]):
        return cls.r.zrem(cls.NAMESPACE, *keys,)

    @classmethod
    def query_range_raw(
        cls,
        start_key: Optional[str] = None,
        stop_key: Optional[str] = None,
        start_inclusive: bool = True,
        stop_inclusive: bool = True,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list:
        kwargs = dict(
            name=cls.NAMESPACE, min="-", max="+", start=None, num=None
        )
        if start_key and stop_key and stop_key == "=":
            stop_key = f"{start_key}\xff"
        if start_key:
            if start_inclusive:
                kwargs["min"] = f"[{start_key}"
            else:
                kwargs["min"] = f"({start_key}"
        if stop_key:
            if stop_inclusive:
                kwargs["max"] = f"[{stop_key}"
            else:
                kwargs["max"] = f"({stop_key}"
        if limit and not offset:
            kwargs["num"], kwargs["start"] = limit, 0
        elif limit and offset:
            kwargs["num"], kwargs["start"] = limit, offset
        return cls.r.zrangebylex(**kwargs)

    @classmethod
    def _parse_response(cls, resp: list) -> list:
        return [item.decode("utf-8") for item in resp]

    @classmethod
    def query_range(
        cls,
        start_key: Optional[str] = None,
        stop_key: Optional[str] = None,
        start_inclusive: bool = True,
        stop_inclusive: bool = True,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list:
        raw_response = cls.query_range_raw(
            start_key=start_key,
            stop_key=stop_key,
            start_inclusive=start_inclusive,
            stop_inclusive=stop_inclusive,
            limit=limit,
            offset=offset,
        )
        return cls._parse_response(raw_response)


    @classmethod
    def query_range_count(
        cls,
        start_key: Optional[str] = None,
        stop_key: Optional[str] = None,
        start_inclusive: bool = True,
        stop_inclusive: bool = True,
    ) -> int:
        kwargs = dict(name=cls.NAMESPACE, min="-", max="+")
        if start_key and stop_key and stop_key == "=":
            stop_key = f"{start_key}\xff"
        if start_key:
            if start_inclusive:
                kwargs["min"] = f"[{start_key}"
            else:
                kwargs["min"] = f"({start_key}"
        if stop_key:
            if stop_inclusive:
                kwargs["max"] = f"[{stop_key}"
            else:
                kwargs["max"] = f"({stop_key}"

        return cls.r.zlexcount(**kwargs)

    @classmethod
    def add_and_remove_keys(
        cls, keys_to_add: List[str], keys_to_remove: List[str]
    ):
        pipe = cls.r.pipeline(transaction=True)
        pipe.zadd(name=cls.NAMESPACE, mapping={k: 0 for k in keys_to_add})
        pipe.zrem(cls.NAMESPACE, *keys_to_remove)
        return pipe.execute()

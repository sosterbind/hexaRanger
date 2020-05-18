from typing import Optional, Tuple, Dict, List
from itertools import permutations
import base
from redis_client import StoreClient

# r = redis.Redis(host="localhost", port=6379, db=0)
# r.set("name", "sharon")
# print(r.get("name"))


class HexaStore(base.HexaStore):

    DELIM = "::"
    PREFIX_DELIM = ":>"

    client = StoreClient


    @classmethod
    def add_item(cls, thing: str, attr: str, val: str):
        keys = cls._to_hexastore_key_set(thing=thing,attr=attr,val=val)
        return cls.client.add_keys(keys)


    @classmethod
    def remove_item(cls, thing: str, attr: str, val: str):
        keys = cls._to_hexastore_key_set(thing=thing,attr=attr,val=val)
        return cls.client.remove_keys(keys)

    @classmethod
    def update_item(cls):
        pass

    @classmethod
    def lookup_items(
        cls,
        thing: Optional[str] = None,
        attr: Optional[str] = None,
        val: Optional[str] = None,
    ) -> list:
        key = cls.get_composite_key(thing=thing, attr=attr, val=val)
        query_response = cls.client.query_range(start_key=key, stop_key="=")
        results = list()
        for entry in query_response:
            thing, attr, val = cls._convert_hexastore_key_to_tuple(entry)
            results.append({"thing": thing, "attr": attr, "val": val})
        return results

    @classmethod
    def lookup_with_groupby(cls):
        pass

    @classmethod
    def count_items(
            cls,
            thing: Optional[str] = None,
            attr: Optional[str] = None,
            val: Optional[str] = None,
    ) -> int:
        key = cls.get_composite_key(thing=thing, attr=attr, val=val)
        return cls.client.query_range_count(start_key=key, stop_key="=")


    @classmethod
    def _to_hexastore_key_set(cls, thing: str, attr: str, val: str) -> List[str]:
        inputs = [
            ("T", thing),
            ("A", attr),
            ("V", val),
        ]
        all_permutations = [p for p in permutations(inputs)]
        hexastore_item = list()
        for permutation in all_permutations:
            prefix_tuple, data_tuple = [tup for tup in zip(*permutation)]
            prefix_str = "".join(prefix_tuple)
            data_str = cls.DELIM.join(data_tuple)
            entry = f"{prefix_str}{cls.PREFIX_DELIM}{data_str}"
            hexastore_item.append(entry)
        return hexastore_item

    @classmethod
    def _convert_hexastore_key_to_tuple(cls, record: str) -> Tuple[str, str, str]:
        prefix_str, data_str = record.split(cls.PREFIX_DELIM)
        ordered_prefixes = list(prefix_str)
        ordered_data = data_str.split(cls.DELIM)
        lookup = {key: val for key, val in zip(ordered_prefixes, ordered_data)}
        return lookup["T"], lookup["A"], lookup["V"]


    @classmethod
    def lookup_things_in_attr_val_range(
            cls,
            attr: Optional[str] = None,
            op: str = "==",
            vals: Optional[List[str]] = None,

    ):
        if op in ["gt", "<"]:
            val = min(vals)
            start_key = cls.get_composite_key(attr=attr, val=f"{val}\xff", partial=True)
            stop_key = cls.get_composite_key(attr=attr, val="\xff", partial=True)
            query_response = cls.client.query_range(
                start_key=start_key,
                stop_key=stop_key,
                start_inclusive=False
            )
        elif op in ["gte", ">="]:
            val = min(vals)
            start_key = cls.get_composite_key(attr=attr, val=f"{val}", partial=True)
            stop_key = cls.get_composite_key(attr=attr, val="\xff", partial=True)
            query_response = cls.client.query_range(
                start_key=start_key,
                stop_key=stop_key,
            )
        elif op in ["lt", "<"]:
            val = max(vals)
            start_key = cls.get_composite_key(attr=attr, val="", partial=True)
            stop_key = cls.get_composite_key(attr=attr, val=val, partial=True)
            query_response = cls.client.query_range(
                start_key=start_key,
                stop_key=stop_key,
                stop_inclusive=False
            )
        elif op in ["lte", "<="]:
            val = max(vals)
            start_key = cls.get_composite_key(attr=attr, val="", partial=True)
            stop_key = cls.get_composite_key(attr=attr, val=f"{val}\xff", partial=True)
            query_response = cls.client.query_range(
                start_key=start_key,
                stop_key=stop_key,
            )
        elif op in ["begins_with", "starts_with"]:
            raise NotImplementedError
        elif op in ["between"]:
            min_val = min(vals)
            max_val = max(vals)
            start_key = cls.get_composite_key(attr=attr, val=min_val, partial=True)
            stop_key = cls.get_composite_key(attr=attr, val=max_val, patial=True)
            query_response = cls.client.query_range(
                start_key=start_key,
                stop_key=stop_key,
            )
        else:
            val = max(vals)
            key = cls.get_composite_key(attr=attr, val=val)
            query_response = cls.client.query_range(start_key=key, stop_key="=")

        results = list()
        for entry in query_response:
            thing, attr, val = cls._convert_hexastore_key_to_tuple(entry)
            results.append({"thing": thing, "attr": attr, "val": val})
        return results

    @classmethod
    def get_composite_key(
            cls,
            thing: Optional[str] = None,
            attr: Optional[str] = None,
            val: Optional[str] = None,
            partial: bool = False,
    ) -> str:
        query_parts = dict(T=thing, A=attr, V=val, )
        prefix = ""
        prefix_end = ""
        elements = []
        for key, val in query_parts.items():
            if val is not None:
                prefix += key
                elements.append(val)
            else:
                prefix_end += key
        data_str = cls.DELIM.join(elements)
        trailing_delim = cls.DELIM if not partial else ""
        composite_key = (
            f"{prefix}{prefix_end}{cls.PREFIX_DELIM}{data_str}{trailing_delim}"
        )
        return composite_key

x = 0
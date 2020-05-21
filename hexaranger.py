from typing import Optional, Tuple, Dict, List, Callable, Any
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
            ordering: str = "TAV",
    ) -> str:
        query_parts = dict(T=thing, A=attr, V=val, )
        prefix = ordering
        elements = []
        for key in list(ordering):
            if query_parts[key] is not None:
                elements.append(val)
            else:
                break
        data_str = cls.DELIM.join(elements)
        composite_key = (
            f"{prefix}{cls.PREFIX_DELIM}{data_str}"
        )
        return composite_key

    @classmethod
    def map_func_to_groups(
        cls,
        func: Callable[[str], Any],
        attr: str,
        val_prefix: Optional[str] = None,
    ):
        ordering = "AVT"
        range_start_key = cls.get_composite_key(
            attr=attr,
            val=val_prefix,
            ordering=ordering,
        )
        # AVT:>birthdate::
        range_end_key = f"{range_start_key}\xff"
        reached_end = False
        group_output = list()
        cursor = range_start_key
        while cursor < range_end_key and not reached_end:
            # AVT:>color::
            try:
                first_key_in_group = cls.client.query_range(start_key=cursor, limit=1)[0]
            except IndexError:
                reached_end = True
                break
            _, attr, val = cls._convert_hexastore_key_to_tuple(first_key_in_group)
            group_prefix = cls.get_composite_key(attr=attr, val=val, ordering=ordering)
            # AVT:>color::orange
            result = func(group_prefix)
            group_output.append((group_prefix, result),)
            # set cursor to last possible key for group
            cursor = f"{group_prefix}\xff"

        return group_output

x = 0
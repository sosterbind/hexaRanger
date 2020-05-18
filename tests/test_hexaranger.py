from hexaranger import HexaStore, StoreClient
import unittest

TEST_NAMESPACE = "test"


class TestStoreClient(StoreClient):
    NAMESPACE = TEST_NAMESPACE


class TestHexaStore(HexaStore):
    client = TestStoreClient


class HexaStoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        super(HexaStoreTestCase, self).setUp()
        self.hs = TestHexaStore

    def _make_some_things(self):
        hs = TestHexaStore
        vals_to_add = [f"{x}" for x in range(10)]
        attrs_to_add = ["attr1", "attr2", "attr3"]
        thing_count = 0
        for i in range(2):
            for attr in attrs_to_add:
                for val in vals_to_add:
                    thing = f"person_{thing_count}"
                    hs.add_item(thing, attr, val)
                    thing_count += 1

    def tearDown(self) -> None:
        self.hs.client.r.delete(TEST_NAMESPACE)
        super(HexaStoreTestCase, self).tearDown()

    def test_add_item(self):
        hs = TestHexaStore
        resp = hs.add_item("bob", "favorite color", "blue")
        item_count = hs.count_items("bob", "favorite color")
        expected = 1
        self.assertEqual(expected, item_count)
        # item_count = hs.lookup_item_count()
        # expected = 1
        # self.assertEqual(expected, item_count)

    def test_lookup_items(self):
        pass

    def test_lookup_item_count(self):
        pass

    def test_to_hexastore_key_set(self):
        pass

    def test_hexastore_key_to_tuple(self):
        pass

    def test_get_composite_key(self):
        pass

    def test_update_item(self):
        pass

    def test_lookup_things_in_attr_val_range_not_implemented_raises_error(self):
        pass


    def test_lookup_things_in_attr_val_range_lt(self):
        self._make_some_things()

        hs = TestHexaStore
        filtered_things = hs.lookup_things_in_attr_val_range(
            attr="attr2",
            op="lt",
            vals=["4"]
        )
        expected_count = 4*2
        self.assertEqual(expected_count, len(filtered_things))

    def test_lookup_things_in_attr_val_range_lte(self):
        self._make_some_things()
        hs = TestHexaStore
        filtered_things = hs.lookup_things_in_attr_val_range(
            attr="attr2",
            op="lte",
            vals=["4"],
        )
        expected_count = 5*2
        self.assertEqual(expected_count, len(filtered_things))

    def test_lookup_things_in_attr_val_range_gt(self):
        self._make_some_things()
        hs = TestHexaStore
        filtered_things = hs.lookup_things_in_attr_val_range(
            attr="attr2",
            op="gt",
            vals=["4"],
        )
        expected_count = 5*2
        self.assertEqual(expected_count, len(filtered_things))

    def test_lookup_things_in_attr_val_range_gte(self):
        self._make_some_things()
        hs = TestHexaStore
        filtered_things = hs.lookup_things_in_attr_val_range(
            attr="attr2",
            op="gte",
            vals=["4"],
        )
        expected_count = 6*2
        self.assertEqual(expected_count, len(filtered_things))

    def test_lookup_thing_in_attr_val_range_eq(self):
        self._make_some_things()
        hs = TestHexaStore
        filtered_things = hs.lookup_things_in_attr_val_range(
            attr="attr2",
            op="eq",
            vals=["4"],
        )
        expected_count = 1*2
        self.assertEqual(expected_count, len(filtered_things))
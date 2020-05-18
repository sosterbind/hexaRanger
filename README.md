# hexaRanger

## Usage

```python
from hexaranger import HexaStore as hs

# add items to store with kwargs
hs.add_item(thing="bob", attr="favorite color", val="blue")
# or with positional args
hs.add_item("alice", "favorite_color", "red")
hs.add_item("tom", "favorite_color", "orange")
hs.add_item("bob", "number_of_clicks", "44")
hs.add_item("alice", "number_of_clicks", "22")
hs.add_item("raj", "number_of_clicks", "33")
hs.add_item("ron", "number_of_clicks", "99")

# query items with variable level of specificity
hs.lookup_items("alice", "favorite_color")
hs.lookup_items("alice")
hs.lookup_items()
# also optionally use kwargs
hs.lookup_items(thing="bob", attr="number_of_clicks")
hs.lookup_items(attr="favorite_color", val="orange")

# using same query structure can also get counts
hs.count_items()
hs.count_items(thing="bob")
hs.count_items(attr="favorite_color", val="orange")

# to update items, provide a lookup and a patch
lookup = dict(thing="bob", attr="favorite_color")
patch = dict(val="orange")
hs.update_item(lookup, patch)
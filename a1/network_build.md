---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.1'
      jupytext_version: 1.2.4
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
import pandas as pd
from collections import defaultdict
```

```python
d = defaultdict(int)
with open('yelp_network.txt', 'r') as f:
    i = -1
    for i, line in enumerate(f):
        u1, u2 = line.strip().split(' ')
        d[u1] += 1
        d[u2] += 1
    n_edges = i + 1
    n_nodes = len(d)
```

```python
data = pd.Series(d)
```

```python
data = data.sort_values(ascending=False)
```

```python
print('#nodes:{} #edges:{}'.format(n_nodes, n_edges))
for i in data.iteritems():
    print('{}:{}'.format(i[0], i[1]))
```

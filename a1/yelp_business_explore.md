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
    display_name: Python [conda env:ml] *
    language: python
    name: conda-env-ml-py
---

```python
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
%matplotlib inline

data = pd.read_csv('yelp_business.csv')
```

```python
data.describe()
```

```python
city_data = data[data['city'] == 'Toronto']
```

```python
city_data
```

```python
city_data.info(memory_usage='deep')
```

```python
del city_data
```

```python
restaurant_data = city_data[city_data['categories'].str.contains('restaurant', case=False)]
```

```python
restaurant_data.info(memory_usage='deep')
```

```python
categories = restaurant_data['categories'].str.split(';').explode()
categories
```

```python
restaurant_data.loc[15]
```

```python
filter = categories.str.contains(pat=r'restaurants*|food', regex=True, case=False)
filter.sum()
```

```python
categories = categories[~filter]
```

```python
restaurantCategoryDist = categories.value_counts()
restaurantCategoryDist
```

```python
for s in restaurantCategoryDist.iteritems():
    print('{}:{}'.format(s[0], s[1]))
```

```python
restaurantReviewDist = pd.DataFrame(index=restaurantCategoryDist.index, columns=['review_count', 'avg_stars'])
```

```python
i = 0
for c in restaurantCategoryDist.index:
    indexes = categories[(categories == c)].index
    restaurantReviewDist.iloc[i,0] = restaurant_data.loc[indexes]['review_count'].sum()
    restaurantReviewDist.iloc[i,1] = restaurant_data.loc[indexes]['stars'].mean()
    i += 1
```

```python
restaurantReviewDist.dtypes
```

```python
restaurant_data[restaurant_data['categories'].str.contains('Nightlife')]['review_count'].sum()
```

```python
restaurant_data.loc[categories[categories == 'Nightlife'].index]

```

```python
restaurantReviewDist = restaurantReviewDist.sort_values(by=['review_count'], ascending=False)
```

```python
restaurantReviewDist
```

```python
restaurantCategoryDist.iloc[0:10].values
```

```python
top10 = restaurantCategoryDist.iloc[0:10]
plt.rcdefaults()
fig, ax = plt.subplots()
ax.barh(top10.index, top10.values, align='center')
ax.set_yticklabels(top10.index)
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xlabel('Number of restaurants')
ax.set_title('Number of restaurants per category')
```

```python

```

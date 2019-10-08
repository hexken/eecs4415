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
users = pd.read_csv('yelp_user.csv', usecols=['user_id', 'friends'])
```

```python
users = users[~users['friends'].str.contains('None')]
```

```python
users = pd.concat([users['user_id'], users['friends'].str.split(', ')], axis=1)
```

```python
friends_dict = pd.Series(users['friends'].values, index=users['user_id']).to_dict()
```

```python
for u in friends_dict.items():
    exit_loop = False
    print(u[0])
    for e in u[1]:
        if e in friends_dict:
            if u[0] not in friends_dict[e]:
                print('---' + u[0] + ' not found in ' + e)
                exit_loop = True
                break
           # if u[0] in friends_dict[e]:
           #     print('+++' + u[0] + ' was found in ' + e)
    if exit_loop:
        break
```

```python
# dict method
with open('yelp_network.txt', 'w') as f:
    for user in friends_dict.items():
        for friend_id in user[1]:
            f.write('{} {}\n'.format(user[0], friend_id))
            # remove current user_id from friend_id's friend list
            if friend_id in friends_dict:
                # sometimes the friend relation is not stored bidrectionally..
                if user[0] in friends_dict[friend_id]:
                    friends_dict[friend_id].remove(user[0])
```

```python
# non dict method
with open('yelp_network.txt', 'w') as f:
    i = 0
    for user in users.itertuples():
        for friend_id in user[2]:
            f.write('{} {}\n'.format(user[1], friend_id))
            #remove current user_id from friend_id's friend list
            users[users['user_id'] == friend_id]['friends'].apply(lambda x: x.remove(user[1]))
        i += 1
```

```python
users.loc[134]
```

```python
users[users['user_id'] == 'h5ERTYn2vQ1QbjTZvfWPaA']['friends'].apply(lambda x: x.remove('jYiZnueCr7gVq9T34xoa7g'))
```

```python
users[users['user_id'] == 'h5ERTYn2vQ1QbjTZvfWPaA']['friends'].tolist()
```

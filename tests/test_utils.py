from fsync import utils

def test_match():

  cases = [["*.idea/*", "./data/filesync/.idea/deployment.xml", True],
           ["fuck", "shit", False],
           ["f?ck", "fuck", True],
           ]

  for wildcard, s, expect in cases:
    assert utils.match(wildcard, s) == expect
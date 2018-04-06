def register():
    return {
        'func3': func3,
        # this will raise exception due to name collision if also load test_plugs2.py:
        # 'func2': func2
        # this is bad practice but shows that alias doesn't have to = name, and
        # that having func2 defined in two plugin files is not a collision
        'func4': func2
    }


def func3(*args, **kwargs):
    return "hello from func3"

def func2(*args, **kwargs):
    for arg in args:
        print("arg: %s" % arg)
    for key, value in kwargs.items():
        print("key: %s value: %s" % (key, value))
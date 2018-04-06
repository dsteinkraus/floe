def register():
    return {
        'func1': func1,
        'func2': func2
    }


def func1(*args, **kwargs):
    print("My name is Ozymandias, King of Kings;")
    print("Look on my Works, ye Mighty, and despair!")
    return "hello from func1"

def func2(*args, **kwargs):
    for arg in args:
        print("arg: %s" % arg)
    for key, value in kwargs.items():
        print("key: %s value: %s" % (key, value))
import importlib.machinery
import os

class Plugin(object):
    def __init__(self):
        self._registry = {}

    # import methods from a .py file (which must have a register function)
    # the file can be located anywhere
    # note: this technique is nominally deprecated in python 3.4, but see
    # https://bugs.python.org/issue21436 for why that isn't a big concern
    # (and how to do it in 3.5+)
    def import_module(self, mod_file):
        loader = importlib.machinery.SourceFileLoader("module.name", mod_file)
        the_module = loader.load_module("module.name")
        # mod = import_module(mod_name)
        register_method = getattr(the_module, 'register')
        for key, value in register_method().items():
            if key in self._registry:
                raise Exception("duplicate plugin method name '%s'" % key)
            self._registry[key] = value

    def call(self, method_name, *args, **kwargs):
        if not method_name in self._registry:
            raise Exception("no plugin method '%s' is registered" % method_name)
        return self._registry[method_name](*args, **kwargs)

    def have(self, method_name):
        return method_name in self._registry

    def get(self, method_name):
        if self.have(method_name):
            return self._registry[method_name]

    def list(self, pretty=False):
        if pretty:
            return "\n".join(self._registry.keys())
        return self._registry.keys()

    # find all .py files in the path and load them
    def import_folder(self, path):
        for dir_name, subdirs, files in os.walk(path):
            for file_name in files:
                if not file_name.endswith('.py'):
                    continue
                if file_name == '__init__.py':
                    continue # only there for static loading
                self.import_module(dir_name + '/' + file_name)

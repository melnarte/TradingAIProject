import ray

def checkinputlisttypes(types=[]):
    def decorator(function):
        def wrapper(*args, **kwargs):
            if len(args) == 0 and len(types) != 0:
                raise AttributeError(function.__name__ + ': Incorrect number of inputs')

            if len(args) != 0:
                if type(args[1]) != list:
                    raise TypeError(function.__name__ + ' inputs must be a list but is type ' + str(type(args[0])))

                if len(args[1]) != len(types):
                    raise AttributeError(function.__name__ + ': Incorrect number of inputs')

                for i in range(len(args[1])):
                    ok = False
                    for t in types[i]:
                        if isinstance(type(args[1][i]), t) or issubclass(type(args[1][i]), t):
                            ok=True
                            break
                    if not ok:
                        raise TypeError(function.__name__ + ' : Input ' + str(i+1)+' is type ' + str(type(args[1][i])) + ' but must be of type '+ str(types[i]))


            res = function(*args, **kwargs)
            return res
        return wrapper
    return decorator

def useray(func):
    def wrapper(*args, **kwargs):
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            res = func(*args, **kwargs)
            ray.shutdown()
        else:
            res = func(*args, **kwargs)
        return res
    return wrapper



if __name__=='__main__':

    @checkinputlisttypes([])
    def test():
        print("Test ran ok")
        return


    test()

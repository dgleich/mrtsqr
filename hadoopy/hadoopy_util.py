import sys
import os

import hadoopy

def get_args(argv):
    args = {}
    for i,arg in enumerate(argv):
        if arg[0] == '-':
            if i+1 < len(argv):
                val = argv[i+1]
            else:
                val = None
            args[arg[1:]] = val
    return args
    
def setstatus(msg):
    print >>sys.stderr, "Status:", msg
    hadoopy.status(msg)    

class SavedOptions:
    """ Save options to pass to derivative hadoopy jobs. 
    
    This class constructs a hadoopy command line environment
    to save all the key, value pairs provided.
    """
    
    def __init__(self):
        self.cache = {}
        self.args = None
        
    def _get_key(self,key,default,typefunc):
        if key in self.cache:
            return typefunc(self.cache[key])
        if self.args is None:
            # this command had better be in the os environment
            val = os.getenv(key)
        else:
            val = self.args.get(key,None)
            
        if val is None:
            if default is None:
                raise NameError(
                    "option '"+key+"' is not a command line "+
                    "or environment option with no default")
            val = default
        else:
            val = typefunc(val)
            
        self.setkey(key,val)
        return val
            
    def getstrkey(self,key,default=None):
        return self._get_key(key,default,str)
        
        
    def getintkey(self,key,default=None):
        return self._get_key(key,default,int)
        
            
    def setkey(self,key,value):
        os.putenv(key,str(value))
        self.cache[key] = value
        
    def cmdenv(self):
        """ This saves all the options to dumbo params. 
        
        For an option to be saved here, it must have been
        "get" or "set" with the options here.
        """
        env = []
        for key,value in self.cache.items():
            env.append(str(key)+'='+str(value))
        return env

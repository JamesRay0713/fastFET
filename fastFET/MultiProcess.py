import time
import multiprocessing

class ProcessingQueue(object):
    
    def __init__(self, nbProcess=None):
        self.queue = []      
        self.running = []    
        self.finish = []     
        self.processes = []  
        self.nbProcess = nbProcess

        for i in range(self.nbProcess):
            self.processes.append(None)
            self.running.append(None)

    def stop(self):
        ''''''
        for i in range(self.nbProcess):
            self.processes[i].terminate()

    def runOnce(self, logger= None ):
        ''''''
        processesAlive = False  
        for i in range(self.nbProcess):
            if(not self.processes[i] is None and self.processes[i].is_alive()): 
                processesAlive = True
            else:
                if(self.running[i]!=None):  
                    self.finish.append(self.running[i])
                    self.running[i] = None
                    #if(self.processes[i].exitcode!=0):
                    #    sys.exit("Subprocess terminated with exit code %i, execution stoped" % (self.processes[i].exitcode))

                if(len(self.queue)>0):  
                    (target, args, kwargs) = self.queue[0]
                    subproc_name= f'{target.__name__}-{i}' 
                    self.processes[i] = multiprocessing.Process(target=target, args=args, kwargs=kwargs, name= subproc_name)
                    self.processes[i].start()   
                    processesAlive = True
                    self.running[i] = (target, args, kwargs)    
                    self.queue.pop(0)   
        return(processesAlive)

    def waitUntilFree(self):
        
        while True:
            for i in range(self.nbProcess):
                if(self.processes[i] is None or not self.processes[i].is_alive()):
                    return
            time.sleep(1)
            
    def join(self):
        for i in range(self.nbProcess):
            if(not self.processes[i] is None):
                self.processes[i].join()

    def run(self, logger= None ):
        
        processesAlive = False  
        try:
            while len(self.queue)>0 or processesAlive:  
                                                        
                processesAlive = self.runOnce()
                #self.runLog()
                time.sleep(0.1)                   
            for i in range(self.nbProcess):     
                if(not self.processes[i] is None):
                    self.processes[i].join()
                    self.processes[i].close()
        except Exception as e:
            for i in range(self.nbProcess):
                if(not self.processes[i] is None):
                    self.processes[i].terminate()
            raise(e)  

    def addProcess(self, target=None, args=(), kwargs={}):
        self.queue.append((target, args, kwargs))

    def formatLog(self, listP):
        i_space = len(str(len(listP)))
        t_space = len("Function")
        a_space = len("Args")
        kw_space = len("Kwargs")
        log = ""

        for i in range(len(listP)):
            if(listP[i]!=None):
                (target, args, kwargs) = listP[i]
                t_space = len(str(target.__name__)) if len(str(target.__name__))>t_space else t_space
                a_space = len(str(args)) if len(str(args))>a_space else a_space
                kw_space = len(str(kwargs)) if len(str(kwargs))>kw_space else kw_space

        vline = ("="*(t_space+a_space+kw_space+13)) + "\n"
        log+= vline
        log += ("|{:<"+str(i_space)+"s}| {:"+str(t_space)+"s} | {:"+str(a_space)+"s} | {:"+str(kw_space)+"s} | \n").format("#","Function","Args","Kwargs")
        descr = "|{:<"+str(i_space)+"d}| {:"+str(t_space)+"s} | {:"+str(a_space)+"s} | {:"+str(kw_space)+"s} | \n"
        log+= vline

        for i in range(len(listP)):
            if(listP[i]!=None):
                (target, args, kwargs) = listP[i]
                log += descr.format(i, target.__name__, str(args), str(kwargs))
            else:
                log += descr.format(i, "Empty", "", "")

        log += vline

        return(log)

    def runLog(self):
        log =  "#######################\n"
        log += "# Queue : Running      \n"
        log += "#######################\n"
        log += self.formatLog(self.running) + "\n"

        log +=  "#######################\n"
        log += "# Queue : Waiting      \n"
        log += "#######################\n"
        log += self.formatLog(self.queue) + "\n"

        log +=  "#######################\n"
        log += "# Queue : Finish      \n"
        log += "#######################\n"
        log += self.formatLog(self.finish) + "\n"




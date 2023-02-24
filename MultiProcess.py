# 该模块被utils, FET 等调用，而不能调用其他模块

import time
import multiprocessing

class ProcessingQueue(object):
    
    def __init__(self, nbProcess=None):
        self.queue = []     # 变长。放入等待执行的`函数名+参数`元组。（一个函数对应一个进程，queue的函数都等着装载进processes）
        self.running = []   # 定长。放入正在执行的`函数名+参数`元组。
        self.finish = []    # 变长。放入运行完了的`函数名+参数`元组。
        self.processes = [] # 定长。一个坑一个进程。running里的`函数`和这里的`进程对象`在位置上一一对应。
        self.nbProcess = nbProcess

        for i in range(self.nbProcess):
            self.processes.append(None)
            self.running.append(None)

    def stop(self):
        '''终止坑位里的所有进程'''
        for i in range(self.nbProcess):
            self.processes[i].terminate()

    def runOnce(self, logger= None ):
        '''- 核心函数：用于刷一遍所有的进程坑位。填坑执行子任务、更新processes(坑位)、running、queue、finish四个list。
        - 返回值 processesAlive== True 的意义：至少有一个坑位的进程还在运行， False，即坑都死了。
        - 该方法需要与self.waitUntilFree 配合使用'''
        processesAlive = False  # 遍历坑位之前，tag初始化为：坑都死了
        for i in range(self.nbProcess):
            if(not self.processes[i] is None and self.processes[i].is_alive()): # 这个`进程坑位`不空、且占坑的进程对象是活的。那tag就要保持活着
                processesAlive = True
            else:   # `进程坑位`空了，或者进程死了，就要把running对应位置的函数元组给清理掉。
                    #   能符合else的条件，一是坑位本来有空，二是由于`processingQueue.waitUntilFree()`等待的结果。
                if(self.running[i]!=None):  # running里有，则清理，并加进finish
                    self.finish.append(self.running[i])
                    self.running[i] = None
                    #if(self.processes[i].exitcode!=0):
                    #    sys.exit("Subprocess terminated with exit code %i, execution stoped" % (self.processes[i].exitcode))

                if(len(self.queue)>0):  # 前面else是说进程坑位空了，那这里看看还有没有排队的，有就新建一个进程对象放进processes
                    (target, args, kwargs) = self.queue[0]
                    subproc_name= f'{target.__name__}-{i}' 
                    self.processes[i] = multiprocessing.Process(target=target, args=args, kwargs=kwargs, name= subproc_name)
                    ## 自此真正执行子进程。
                    self.processes[i].start()   # 进程对象放入坑位之后，立马开工。自此执行target函数(dataCollect, 或 transform)
                    processesAlive = True
                    self.running[i] = (target, args, kwargs)    # 正在开工执行的函数，放入running
                    self.queue.pop(0)   # 出队
        return(processesAlive)

    def waitUntilFree(self):
        '''- 用于空转，原地等待的函数。不断地遍历`进程坑位`。
        - 退出空转条件：坑位上有空位了。我才去执行`processingQueue.runOnce()`'''
        while True:     # 
            for i in range(self.nbProcess):
                if(self.processes[i] is None or not self.processes[i].is_alive()):
                    return
            time.sleep(1)
            
    def join(self):
        '''等待坑位里的所有子进程都执行完毕'''
        for i in range(self.nbProcess):
            if(not self.processes[i] is None):
                self.processes[i].join()

    def run(self, logger= None ):
        """`self.queue中所有排队的任务的完整执行过程。不断调用了runOnce()`
        - 注：每次遍历坑位，都有0.1s的睡眠。如果子任务复杂度很小数量很大，则非常耗时，且进程间开销是没有必要的。
        """
        processesAlive = False  # 初始化为：False坑都死了。True表明存在活着的坑。
        try:
            while len(self.queue)>0 or processesAlive:  # 只要有排队的，或者`存在活着的坑`时，就要遍历坑位
                                                        # 退出while条件：没有排队的，且坑都死了。
                processesAlive = self.runOnce()
                #self.runLog()
                time.sleep(0.1)                   # 作用相当于跟runOnce()配合使用的waitUntilFree()$$$$$$$$$$$$$$$$$$$$$$$$$$$大问题：睡得太久。得看BML源文档。
            for i in range(self.nbProcess):     # 用于清坑。queue里没有任务了，坑里还剩最后几个，要给他们同步一下。
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




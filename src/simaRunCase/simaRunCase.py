import simapy.sima as sima
import simapy.sima.workflow
from simapy import sre
from simapy.sima_reader import SIMAReader
from simapy.sima_writer import SIMAWriter

import multiprocessing
from functools import partial
from pathlib import Path

import copy
import os
import shutil

import subprocess
import glob




class SimaRunCase():
    '''
    The class sets up a way to set values for variables defined in sima, run the case  
    and return the output hdf5 file
    
    baseJsonFile - the path to the base json file 
    variableList - list of variables in sima model which forms the the design variables set x
    '''
        
    def __init__(self, baseJsonFile, baseWorkspace, addStaskFile = None):
        self.baseWorkspace = baseWorkspace
        os.makedirs(self.baseWorkspace, exist_ok=True)
        shutil.copy(baseJsonFile, self.baseWorkspace) 
        if addStaskFile:
            shutil.copy(addStaskFile, self.baseWorkspace)
            self.addStaskFile = os.path.join(self.baseWorkspace, os.path.basename(addStaskFile))
        else :
            self.addStaskFile = None
        
        

        #Read the base sima json file
        self.simaTask = SIMAReader().read(os.path.join(self.baseWorkspace, os.path.basename(baseJsonFile)))[0]


    #---------------------------sima json runner functions--------------------------------

    @staticmethod
    def run_sima(workspace, commands):
        """Run SIMA with the given workspace directory and command line arguments.

        The function uses an environmental variable SRE_EXE to locate the SIMA installation,
        make sure it is set before running.
        """
        # Requires that the environment is set, but an alternative path may be given
        exe = os.getenv("SRE_EXE")
        sima = sre.SIMA(exe=exe)
        sima.run(workspace, commands)

    @staticmethod
    def runWorkFlow(workspace_dir, json_file, workFlowTask, workFlow, addStaskFile=None, copyHdf5Flag = False, deleteRunFol = False):
        '''
        copyHdf5Flag : the hdf5 file is copied to a results folder located just above the workspace directory
        '''

        workspace = Path(workspace_dir) # workspace directory

        shutil.rmtree(workspace, ignore_errors=True)
        os.makedirs(workspace, exist_ok=True)
        json = Path(json_file) # json file directory
        commands = []
        commands.append("--import")
        if addStaskFile: #import additinal stask file if required - can be used to host any storage task required for the model
            addStaskFile = Path(addStaskFile)
            commands.append("file=" + str(addStaskFile.absolute()))
            commands.append("--import")
        commands.append("file=" + str(json.absolute()))
        commands.append('--run')
        commands.append(f'task={workFlowTask}')
        commands.append(f'workflow={workFlow}')
        SimaRunCase.run_sima(workspace, commands)

        if copyHdf5Flag:
            sourceFile = glob.glob(os.path.join(workspace_dir, workFlowTask, workFlow, '*', os.path.basename(workspace_dir) + '.h5'))
            if len(sourceFile) == 1:
                sourceFile = sourceFile[0]
                os.makedirs(os.path.join(workspace_dir, '../../h5Results'), exist_ok=True)
                if os.path.exists(os.path.join(workspace_dir, '../../h5Results', os.path.basename(sourceFile))):
                    os.remove(os.path.join(workspace_dir, '../../h5Results', os.path.basename(sourceFile)))
                shutil.move(sourceFile, os.path.join(workspace_dir, '../../h5Results'))
                
                if deleteRunFol:
                    shutil.rmtree(workspace_dir, ignore_errors=True)

    #---------------------------json creation functions--------------------------------

    @staticmethod
    def appendReplaceObj(objectIn, objList, attribute='name'):

        """
        Function to append or replace an object in the object list based on checking if an attribute of the object is already present in the list.
        """
        
        attribute_value = getattr(objectIn, attribute)

        if objList == None :
            return [objectIn]
        
        objects_by_attribute = {getattr(obj, attribute): obj for obj in objList} #create a dictionary with the attribute as the key and the object as the value
        
        if attribute_value in objects_by_attribute:
            
            existing_object = objects_by_attribute[attribute_value]
            
            objList[objList.index(existing_object)] = objectIn
        
        else:
            objList.append(objectIn)
        
        return objList



    @staticmethod
    def returnObj2(objectId, objList, identifier='name'):

        '''
        Returns the object in ObjList with the specified ID for the indentifier
        More robust implementation of returnObj function - need to update code to use this intead of returnObj
        '''
        
        idList        = []
        mathchObjList = []
        
        if objList and isinstance(objList[0], dict): #check if the list is a list of dictionaries
            dictListFlag = True
        
        else:
            dictListFlag = False

    
        for obj in objList:

            try :# to avoid raising an error if the attribute does not exist for some of the objects in the list
                
                if dictListFlag:
                    itemID = obj[identifier]
                else:
                    itemID = getattr(obj, identifier) 


            except : 
                print(f'Object {obj} does not have attribute {identifier}')

            else :
                if itemID != None: #the names can also be none type object - in that case ignore it, else will create issues while checking for uniqueness below
                    idList.append(itemID)
                    mathchObjList.append(obj)


        #check if the list is unique
        if len(idList) != len(set(idList)):
            raise Exception(f'List of {identifier} is not unique')
        
        #find the index of the object with the specified ID
        try :
            ind = idList.index(objectId)

        except :
            raise Exception(f'Object with {identifier}={objectId} not found in the list')
        

        return objList[ind]


    @staticmethod
    def getVariable(simaTask, var) :

        '''
        Returns a variable from the simaTask object by interatively searching through the through the variable containers
        '''

        variable_containers = {
                                'double': simaTask.doubleVariables,
                                'integer': simaTask.integerVariables,
                                'string': simaTask.stringVariables
                                }                       

        simaVar  = None

        for var_type, container in variable_containers.items():
            
            try:
                simaVar = SimaRunCase.returnObj2(var, container, identifier='name')
                break  # Exit loop if variable is found
            
            except:
                continue  # Continue to next container if variable is not found
        
        return simaVar

    @staticmethod
    def addConditionSet(task, condName, envCond, varList):
    
        '''
        varList = [{varName : 'name', valList : []}]

        __name__ is a reserved keyword for the name of the condition so its removed from the variable list
        '''

        variablelist = [SimaRunCase.getVariable(task, item) for item in varList.keys() if item != '__name__']    
        valList = [[str(varList[f'{key}'])] for key in varList.keys() if key != '__name__'] #convert to a string and a list
        
        
        varItemList = []

        for var, val in zip(variablelist, valList):
            varItemList.append(sima.condition.VariableItemSet(variable = var, variations = val))


            
        condSet = sima.condition.ConditionSet   (
                                                        name = condName,
                                                        selection = SimaRunCase.returnObj2(envCond, task.model.environments),
                                                        variableItemSets = varItemList
                                                        )

        task.conditions = SimaRunCase.appendReplaceObj(condSet, task.conditions, attribute='name')

    @staticmethod
    def writeSimaJson(simaModel, outPath, openPathFlag = False):
        writer = SIMAWriter()
        writer.write(simaModel, outPath)
        
        if openPathFlag:
            subprocess.Popen(f'start /MAX explorer /select,{outPath}', shell=True)



    @staticmethod
    def addWorkFlow(task, resultFileName, analysis = 'dynamic', condName = 'condition1', wfName = 'wf', wfTaskName = 'wfTask'):

        '''
        Function to generate a simple workflow to run a condition and produce the results as a HDF5 file
        '''
        

        #create  the workflow 
        nodeList          = []
        connections       = []


        simaCondNode  = sima.workflow.ConditionInputNode(
                                                        x                  = 0,
                                                        y                  = 0,
                                                        condition          = SimaRunCase.returnObj2(condName, task.conditions),
                                                        analysis           = analysis,
                                                        outputSlot         = sima.post.OutputSlot()
                                                    )

        nodeList.append(simaCondNode)




        #file output node

        #condition node
        fileOutput = [  #'sima_stamod.inp',
                        # 'sima_stamod.inp',
                        #'sima_eigmod.inp',
                        # 'sima_inpmod.inp',
                        #'simo_dynmod_input.inp'
                        #'simores.inp',
                        # 'stamod.inp',
                        'psc-sima.lis',
                        'psc.lis',
                        'sima_stamod.res',
                        'sima_inpmod.res',
                        'sima_stamod.mpf',
                        'sima_eigmod.res',
                        'sima_eigmod.mpf',
                        'eig.lis',
                        'sima_dynmod.res',
                        #'sys-sima.dat',
                        ]
        

        fileOutputNode = sima.workflow.FileOutputNode(   
                                                        x                           =   500,
                                                        y                           =   0,
                                                        inputSlot                   =   sima.post.InputSlot(),
                                                        outputSlot                  =   sima.post.OutputSlot(),
                                                        fileFormat                  =   sima.post.FileFormat.HDF5,
                                                        filePath                    =   f'{resultFileName}.h5',
                                                        additionalFiles             = [sima.workflow.FileSpecification(filename = item) for item in fileOutput]
                                                    )    


        nodeList.append(fileOutputNode)

        connections.append(sima.post.SlotConnection(    fromSlot = simaCondNode.outputSlot,
                                                        toSlot   = fileOutputNode.inputSlot)) #condition to file output node
        
                
        wf = sima.workflow.Workflow(    name        = wfName, 
                                        nodes       = nodeList,
                                        connections = connections)    


        #create the workflow task
        wfTask = sima.workflow.WorkflowTask(name = wfTaskName, workflows = [wf])

        return wfTask



    @staticmethod
    def addRunCondition(inpDict, simaTask, outFolder, envCond, wfTaskName = 'testWF_task', wfName = 'testWF', condName = 'testCond', analysis = 'dynamic'):

        '''
        Adds a condition set and a work flow 
        '''

        SimaRunCase.addConditionSet(task = simaTask, condName = condName, envCond = envCond, varList = inpDict)

        wfTask = SimaRunCase.addWorkFlow(simaTask, resultFileName = inpDict['__name__'], analysis = analysis, condName = condName, wfName = wfName, wfTaskName = wfTaskName)

        os.makedirs(outFolder, exist_ok=True)

        #write the modifed model
        SimaRunCase.writeSimaJson([simaTask, wfTask], os.path.join(outFolder, f'{inpDict["__name__"]}.json'), openPathFlag = False)

    
    def evalSima(self, varDict, envCond, wfTaskName = 'testWF_task', wfName = 'testWF', condName = 'testCond', analysis = 'dynamic', deleteRunFol = False):
        '''
        Function to evaluate the sima model with the sima variables set in the varDict
        
        '__name__' is the runID  used to identify the name of the output HDF5 file
        '''

        simaTask = copy.deepcopy(self.simaTask) #Keep the bse json file as it is

        #add run variables, condition set, workflow and write the json file
        SimaRunCase.addRunCondition(varDict, simaTask, os.path.join(self.baseWorkspace, 'jsonFiles'), envCond, 
                                    wfTaskName = 'testWF_task', wfName = 'testWF', condName = 'testCond', analysis = analysis)
        
        #run the json file
        SimaRunCase.runWorkFlow(workspace_dir = os.path.join(self.baseWorkspace, 'runFolder', varDict["__name__"]), 
                                json_file = os.path.join(self.baseWorkspace, 'jsonFiles', f'{varDict["__name__"]}.json'),
                                workFlowTask = wfTaskName, 
                                workFlow = wfName, 
                                addStaskFile=self.addStaskFile, 
                                copyHdf5Flag = True,
                                deleteRunFol = deleteRunFol)
        


def runCases(simRunObj, varDictList, envCond, maxCores = multiprocessing.cpu_count()//2, wfTaskName = 'testWF_task', wfName = 'testWF', 
             condName = 'testCond', analysis = 'dynamic', deleteRunFol = False):
    
    nCores = min(maxCores, multiprocessing.cpu_count()-2, len(varDictList))
    
    with multiprocessing.Pool(nCores) as pool:
        pool.map(partial(simRunObj.evalSima, envCond=envCond, wfTaskName = wfTaskName, 
                         wfName = wfName, condName = condName, analysis = analysis, deleteRunFol = deleteRunFol), varDictList)
        
    
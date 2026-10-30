# Databricks notebook source
# MAGIC %md
# MAGIC #Create functions for running notebooks in parallel

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def printNotebookInfo(notebook, success):
    displayed=False
    
    try:
        if notebook.databasename is not None\
        and notebook.audittablename is not None\
        and notebook.notebooktablename is not None:
            x = spark.sql("select runId as runId, jobId as jobId from "\
            +notebook.databasename+"."+notebook.audittablename\
            +" where upper(TableName) = '"+notebook.notebooktablename.upper()+"'").collect()
            if len(x) > 0:
                if success:
                    print("Finished Notebook "+notebook.path + "(JobId = " + x[0]["jobId"]+")")
                    displayed=True
                else:
                    print("Error in Notebook "+notebook.path + "(JobId = " + x[0]["jobId"]+")")
                    displayed=True
    except Exception as err:
        z=err#do nothing
    if not displayed:
        if success:
            print("Finished notebook %s" % notebook.path)
        else:
            print("Error in notebook %s" % notebook.path)

class NotebookData:
    def __init__(self, path, timeout, parameters=None, retry=0, databasename=None, audittablename=None, notebooktablename=None):
        self.path = path
        self.timeout = timeout
        self.parameters = parameters
        self.retry = retry
        self.databasename = databasename
        self.audittablename = audittablename
        self.notebooktablename = notebooktablename
        self.IsDone = False
        self.IsError = False

    def submitNotebook(notebook):
        print("Running notebook %s" % notebook.path)
        try:
            if (notebook.parameters):
                notebookresult = dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
                printNotebookInfo(notebook, True)
                #print("Finished notebook %s" % notebook.path)
                notebook.IsDone = True
                return notebookresult
            else:
                notebookresult = dbutils.notebook.run(notebook.path, notebook.timeout)
                printNotebookInfo(notebook, True)
                #print("Finished notebook %s" % notebook.path)
                notebook.IsDone = True
                return notebookresult
        except Exception as err:
            printNotebookInfo(notebook, False)
            #print("Error in notebook %s" % notebook.path)
            #print("Error:")
            #print(err)
            if notebook.retry < 1:
                notebook.IsError=True
                raise
        print("Retrying notebook %s" % notebook.path)
        notebook.retry = notebook.retry - 1
        submitNotebook(notebook)
def parallelNotebooks(notebooks, numInParallel):
   # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
   # This code limits the number of parallel notebooks.
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(NotebookData.submitNotebook, notebook) for notebook in notebooks]
#Array of instances of NotebookData Class



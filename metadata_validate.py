#main_script.py
import subprocess
import os 
import datetime
runTime=datetime.datetime.now()
runID=runTime.strftime("%Y%m%d%H%M%S%f%j")
print(runTime)
print(runID)

if __name__ == "__main__":
    print('Running Data Profile ')
    from qa import profiling
    profiling(runID,runTime)

SNS Refinement Workflow
=======================

Usage
-----
1. Create/edit configuration file (e.g. test.cfg)
    a. Set the application parameters
    b. Set the job sizes

2. Edit sites.xml:
    a. Set the path to your scratch directory
    b. Set the path to your output directory
    c. Set the project number

3. Run plan.sh to plan and submit workflow:

    $ ./plan.sh myrun test.cfg
    
    to vary temperature
    
    or
    
    $ ./plan.sh myrun test.cfg --synthetic

    to generate a synthetic version of workflow.

    NOTE: be sure to have all pfns in tc-fake.txt file set to pegasus(-mpi)-keg when planning a synthetic workflow.
    
    or 
    
    $ ./plan.sh myrun test.cfg --hydrogen
    
    to vary hydrogen charge.

4. Get NERSC grid proxy using:

    $ myproxy-logon -s nerscca.nersc.gov:7512 -t 720 -T -l YOUR_NERSC_USERNAME

5. Monitor the workflow:

    $ pegasus-status -l myrun/submit/.../run0001


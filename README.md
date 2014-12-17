SNS Refinement Workflow
=======================

Usage
-----
1. Create/edit configuration file (e.g. test.cfg)
   In sites.xml replace juve with your NERSC user name.

2. Run daxgen.py to generate workflow in a given directory (e.g. myrun):

    $ python daxgen.py test.cfg myrun

    to vary temperature
    
    or
    
    $ python daxgen.py --synthetic test.cfg myrun

    to generate a synthetic version of workflow.

    or
    
    $ python daxgenQ.py testQ.cfg myrun

    to vary hydrogen charge.

3. Run plan.sh to plan workflow:

    $ ./plan.sh myrun

    NOTE: be sure to have all pfns in tc.txt file set to pegasus(-mpi)-keg when planning a synthetic workflow.
    
4. Get NERSC grid proxy using:

    $ myproxy-logon -s nerscca.nersc.gov:7512 -t 720 -T -l YOUR_NERSC_USERNAME

5. Follow output of plan.sh to submit workflow

    $ pegasus-run myrun/submit/.../run0001

6. Monitor the workflow:

    $ pegasus-status -l myrun/submit/.../run0001


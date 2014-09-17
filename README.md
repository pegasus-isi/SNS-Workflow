SNS Refinement Workflow
=======================

Usage
-----
1. Create/edit configuration file (e.g. test.cfg)

2. Run daxgen.py to generate workflow in a given directory (e.g. myrun):

    $ python daxgen.py test.cfg myrun

3. Run plan.sh to plan workflow:

    $ ./plan.sh myrun

4. Get NERSC grid proxy using:

    $ myproxy-logon -s nerscca.nersc.gov:7512 -t 24 -T -l YOUR_NERSC_USERNAME

5. Follow output of plan.sh to submit workflow

    $ pegasus-run myrun/submit/.../run0001

6. Monitor the workflow:

    $ pegasus-status -l myrun/submit/.../run0001


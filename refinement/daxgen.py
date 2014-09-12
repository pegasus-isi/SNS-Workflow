import sys
from ConfigParser import ConfigParser
from Pegasus.DAX3 import *

class RefinementWorkflow(object):
    def __init__(self, config):
        self.config = config
        self.mink = config.getfloat("simulation", "mink")
        self.kstep = config.getfloat("simulation", "kstep")
        self.ksteps = config.getint("simulation", "ksteps")

        if self.kstep <= 0.0:
            raise Exception("Invalid value for simulation.kstep: %f" % self.kstep)

        if self.ksteps <= 0:
            raise Exception("Invalid value for simulation.ksteps: %d" % self.ksteps)

    def writeDAX(self, output):
        dax = ADAG("refinement")

        coherent_files = []
        incoherent_files = []

        coherent_jobs = []
        incoherent_jobs = []

        experiment = File("experiment_results.dat")

        k = self.mink
        for i in range(0, self.ksteps):
            param = File("k%s.param" % k)
            traj_eq = File("k%s_equilibrium.trajectory" % k)
            traj_prod = File("k%s_production.trajectory" % k)
            inco = File("k%s_incoherent.dat" % k)
            co = File("k%s_coherent.dat" % k)

            eqjob = Job("NAMD")
            eqjob.addArguments("--config", param)
            eqjob.uses(param, link=Link.INPUT)
            eqjob.uses(traj_eq, link=Link.OUTPUT)
            dax.addJob(eqjob)

            prodjob = Job("NAMD")
            prodjob.addArguments("--config", param)
            prodjob.uses(param, link=Link.INPUT)
            prodjob.uses(traj_eq, link=Link.INPUT)
            prodjob.uses(traj_prod, link=Link.OUTPUT)
            dax.addJob(prodjob)

            dax.depends(prodjob, eqjob)

            incojob = Job("SASSENA")
            incojob.addArguments("--incoherent", "--output", inco, "--input", traj_prod)
            incojob.uses(traj_prod, link=Link.INPUT)
            incojob.uses(inco, link=Link.OUTPUT)
            dax.addJob(incojob)

            dax.depends(incojob, prodjob)

            cojob = Job("SASSENA")
            cojob.addArguments("--coherent", "--output", co, "--input", traj_prod)
            cojob.uses(traj_prod, link=Link.INPUT)
            cojob.uses(co, link=Link.OUTPUT)
            dax.addJob(cojob)

            dax.depends(cojob, prodjob)

            incoherent_files.append(inco)
            incoherent_jobs.append(incojob)
            coherent_files.append(co)
            coherent_jobs.append(cojob)

            k += self.kstep

        siqe = File("siqe.dat")
        sqek = File("sqek.dat")
        sqekstar = File("sqekstar.dat")

        siqejob = Job("MANTID")
        siqejob.addArguments("--output", siqe)
        siqejob.addArguments(*incoherent_files)
        siqejob.addArguments(*coherent_files)
        for inco in incoherent_files:
            siqejob.uses(inco, link=Link.INPUT)
        for co in coherent_files:
            siqejob.uses(co, link=Link.INPUT)
        siqejob.uses(siqe, link=Link.OUTPUT)
        dax.addJob(siqejob)

        for incojob in incoherent_jobs:
            dax.depends(siqejob, incojob)
        for cojob in coherent_jobs:
            dax.depends(siqejob, cojob)

        sqekjob = Job("MANTID")
        sqekjob.addArguments("--input", siqe, "--experiment", experiment, "--output", sqek)
        sqekjob.uses(siqe, link=Link.INPUT)
        sqekjob.uses(experiment, link=Link.INPUT)
        sqekjob.uses(sqek, link=Link.OUTPUT)
        dax.addJob(sqekjob)

        dax.depends(sqekjob, siqejob)

        sqekstarjob = Job("MANTID")
        sqekstarjob.addArguments("--input", sqek, "--output", sqekstar)
        sqekstarjob.uses(sqek, link=Link.INPUT)
        sqekstarjob.uses(sqekstar, link=Link.OUTPUT, transfer=True)
        dax.addJob(sqekstarjob)

        dax.depends(sqekstarjob, sqekjob)

        dax.writeXML(output)


def main():
    if len(sys.argv) != 2:
        raise Exception("Usage: %s CONFIGFILE" % sys.argv[0])

    configfile = sys.argv[1]
    if not os.path.isfile(configfile):
        raise Exception("No such file: %s" % configfile)

    config = ConfigParser()
    config.read(configfile)

    workflow = RefinementWorkflow(config)

    workflow.writeDAX(sys.stdout)


if __name__ == '__main__':
        main()


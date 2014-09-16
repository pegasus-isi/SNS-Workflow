import sys
import string
from ConfigParser import ConfigParser
from Pegasus.DAX3 import *

DAXGEN_DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DAXGEN_DIR, "templates")

def format_template(name, outfile, **kwargs):
    templatefile = os.path.join(TEMPLATE_DIR, name)
    template = open(templatefile).read()
    formatter = string.Formatter()
    data = formatter.format(template, **kwargs)
    f = open(outfile, "w")
    try:
        f.write(data)
    finally:
        f.close()

class RefinementWorkflow(object):
    def __init__(self, outdir, config):
        self.outdir = outdir
        self.config = config
        self.daxfile = os.path.join(self.outdir, "dax.xml")
        self.replicas = {}

        self.temperatures = [x.strip() for x in config.get("simulation", "temperatures").split(",")]
        self.equilibrate_steps = config.get("simulation", "equilibrate_steps")
        self.production_steps = config.get("simulation", "production_steps")
        self.pressure = config.get("simulation", "pressure")
        self.charge = config.get("simulation", "charge")
        self.structure = config.get("simulation", "structure")
        self.coordinates = config.get("simulation", "coordinates")
        self.parameters = config.get("simulation", "parameters")
        self.topfile = config.get("simulation", "topfile")
        self.extended_system = config.get("simulation", "extended_system")
        self.sassena_db = config.get("simulation", "sassena_db")
        self.incoherent_db = "database/db-neutron-incoherent.xml"
        self.coherent_db = "database/db-neutron-coherent.xml"

    def add_replica(self, name, path):
        url = "file://%s" % path
        self.replicas[name] = url

    def generate_replica_catalog(self):
        path = os.path.join(self.outdir, "rc.txt")
        f = open(path, "w")
        try:
            for name, url in self.replicas.items():
                f.write('%s    %s    pool="local"\n' % (name, url))
        finally:
            f.close()

    def generate_eq_conf(self, temperature):
        name = "equilibrate_%s.conf" % temperature
        path = os.path.join(self.outdir, name)
        kw = {
            "temperature": temperature,
            "pressure": self.pressure,
            "charge": self.charge,
            "structure": self.structure,
            "coordinates": self.coordinates,
            "parameters": self.parameters,
            "outputname": "equilibrate_%s" % temperature,
            "extended_system": self.extended_system,
            "timesteps": self.equilibrate_steps
        }
        format_template("equilibrate.conf", path, **kw)
        self.add_replica(name, path)

    def generate_prod_conf(self, temperature):
        name = "production_%s.conf" % temperature
        path = os.path.join(self.outdir, name)
        kw = {
            "temperature": temperature,
            "pressure": self.pressure,
            "charge": self.charge,
            "structure": self.structure,
            "coordinates": self.coordinates,
            "parameters": self.parameters,
            "inputname": "equilibrate_%s" % temperature,
            "outputname": "production_%s" % temperature,
            "timesteps": self.production_steps
        }
        format_template("production.conf", path, **kw)
        self.add_replica(name, path)

    def generate_ptraj_conf(self, temperature):
        name = "ptraj_%s.conf" % temperature
        path = os.path.join(self.outdir, name)
        kw = {
            "trajectory_input": "production_%s.dcd" % temperature,
            "trajectory_fit": "ptraj_%s.fit" % temperature,
            "trajectory_output": "ptraj_%s.dcd" % temperature
        }
        format_template("rms2first.ptraj", path, **kw)
        self.add_replica(name, path)

    def generate_incoherent_conf(self, temperature):
        name = "sassenaInc_%s.xml" % temperature
        path = os.path.join(self.outdir, name)
        kw = {
            "coordinates": self.coordinates,
            "trajectory": "ptraj_%s.dcd" % temperature,
            "output": "fqt_inc_%s.hd5" % temperature,
            "database": self.incoherent_db
        }
        format_template("sassenaInc.xml", path, **kw)
        self.add_replica(name, path)

    def generate_coherent_conf(self, temperature):
        name = "sassenaCoh_%s.xml" % temperature
        path = os.path.join(self.outdir, name)
        kw = {
            "coordinates": self.coordinates,
            "trajectory": "ptraj_%s.dcd" % temperature,
            "output": "fqt_coh_%s.hd5" % temperature,
            "database": self.coherent_db
        }
        format_template("sassenaCoh.xml", path, **kw)
        self.add_replica(name, path)

    def generate_workflow(self):
        dax = ADAG("refinement")

        structure = File(self.structure)
        coordinates = File(self.coordinates)
        parameters = File(self.parameters)
        extended_system = File(self.extended_system)
        topfile = File(self.topfile)
        sassena_db = File(self.sassena_db)
        incoherent_db = File(self.incoherent_db)
        coherent_db = File(self.coherent_db)

        untarjob = Job("tar")
        untarjob.addArguments("-xzvf", sassena_db)
        untarjob.uses(sassena_db, link=Link.INPUT)
        untarjob.uses(incoherent_db, link=Link.OUTPUT, transfer=False)
        untarjob.uses(coherent_db, link=Link.OUTPUT, transfer=False)
        untarjob.profile("globus", "jobtype", "single")
        untarjob.profile("globus", "maxwalltime", "1")
        untarjob.profile("globus", "count", "1")
        dax.addJob(untarjob)

        for temperature in self.temperatures:
            # Equilibrate files
            eq_conf = File("equilibrate_%s.conf" % temperature)
            eq_coord = File("equilibrate_%s.restart.coord" % temperature)
            eq_xsc = File("equilibrate_%s.restart.xsc" % temperature)
            eq_vel = File("equilibrate_%s.restart.vel" % temperature)

            # Production files
            prod_conf = File("production_%s.conf" % temperature)
            prod_dcd = File("production_%s.dcd" % temperature)

            # Ptraj files
            ptraj_conf = File("ptraj_%s.conf" % temperature)
            ptraj_fit = File("ptraj_%s.fit" % temperature)
            ptraj_dcd = File("ptraj_%s.dcd" % temperature)

            # Sassena incoherent files
            incoherent_conf = File("sassenaInc_%s.xml" % temperature)
            fqt_incoherent = File("fqt_inc_%s.hd5" % temperature)

            # Sassena coherent files
            coherent_conf = File("sassenaCoh_%s.xml" % temperature)
            fqt_coherent = File("fqt_coh_%s.hd5" % temperature)

            # Generate configuration files for this pipeline
            self.generate_eq_conf(temperature)
            self.generate_prod_conf(temperature)
            self.generate_ptraj_conf(temperature)
            self.generate_incoherent_conf(temperature)
            self.generate_coherent_conf(temperature)

            # Equilibrate job
            eqjob = Job("namd")
            eqjob.addArguments(eq_conf)
            eqjob.uses(eq_conf, link=Link.INPUT)
            eqjob.uses(eq_coord, link=Link.OUTPUT, transfer=False)
            eqjob.uses(eq_xsc, link=Link.OUTPUT, transfer=False)
            eqjob.uses(eq_vel, link=Link.OUTPUT, transfer=False)
            eqjob.profile("globus", "jobtype", "mpi")
            eqjob.profile("globus", "maxwalltime", "60")
            eqjob.profile("globus", "count", "288")
            dax.addJob(eqjob)

            # Production job
            prodjob = Job("namd")
            prodjob.addArguments(prod_conf)
            prodjob.uses(prod_conf, link=Link.INPUT)
            prodjob.uses(eq_coord, link=Link.INPUT)
            prodjob.uses(eq_xsc, link=Link.INPUT)
            prodjob.uses(eq_vel, link=Link.INPUT)
            prodjob.uses(prod_dcd, link=Link.OUTPUT)
            prodjob.profile("globus", "jobtype", "mpi")
            prodjob.profile("globus", "maxwalltime", "360")
            prodjob.profile("globus", "count", "288")
            dax.addJob(prodjob)
            dax.depends(prodjob, eqjob)

            # ptraj job
            ptrajjob = Job(namespace="amber", name="ptraj")
            ptrajjob.addArguments(topfile)
            ptrajjob.setStdin(ptraj_conf)
            ptrajjob.uses(topfile, link=Link.INPUT)
            ptrajjob.uses(ptraj_conf, link=Link.INPUT)
            ptrajjob.uses(prod_dcd, link=Link.INPUT)
            ptrajjob.uses(ptraj_fit, link=Link.OUTPUT)
            ptrajjob.uses(ptraj_dcd, link=Link.OUTPUT)
            ptrajjob.profile("globus", "jobtype", "single")
            ptrajjob.profile("globus", "maxwalltime", "60")
            ptrajjob.profile("globus", "count", "1")
            dax.addJob(ptrajjob)
            dax.depends(ptrajjob, prodjob)

            # sassena incoherent job
            incojob = Job("sassena")
            incojob.addArguments("--config", incoherent_conf)
            incojob.uses(incoherent_conf, link=Link.INPUT)
            incojob.uses(ptraj_dcd, link=Link.INPUT)
            incojob.uses(incoherent_db, link=Link.INPUT)
            incojob.uses(fqt_incoherent, link=Link.OUTPUT)
            incojob.profile("globus", "jobtype", "mpi")
            incojob.profile("globus", "maxwalltime", "360")
            incojob.profile("globus", "count", "144")
            dax.addJob(incojob)
            dax.depends(incojob, ptrajjob)
            dax.depends(incojob, untarjob)

            # sassena coherent job
            cojob = Job("sassena")
            cojob.addArguments("--config", coherent_conf)
            cojob.uses(coherent_conf, link=Link.INPUT)
            cojob.uses(ptraj_dcd, link=Link.INPUT)
            cojob.uses(coherent_db, link=Link.INPUT)
            cojob.uses(fqt_coherent, link=Link.OUTPUT)
            cojob.profile("globus", "jobtype", "mpi")
            cojob.profile("globus", "maxwalltime", "360")
            cojob.profile("globus", "count", "144")
            dax.addJob(cojob)
            dax.depends(cojob, prodjob)
            dax.depends(cojob, untarjob)

        dax.writeXMLFile(self.daxfile)

        self.generate_replica_catalog()

def main():
    if len(sys.argv) != 3:
        raise Exception("Usage: %s CONFIGFILE OUTDIR" % sys.argv[0])

    configfile = sys.argv[1]
    outdir = sys.argv[2]

    if not os.path.isfile(configfile):
        raise Exception("No such file: %s" % configfile)

    if os.path.isdir(outdir):
        raise Exception("Directory exists: %s" % outdir)

    outdir = os.path.abspath(outdir)
    os.makedirs(outdir)

    config = ConfigParser()
    config.read(configfile)

    workflow = RefinementWorkflow(outdir, config)
    workflow.generate_workflow()


if __name__ == '__main__':
    main()


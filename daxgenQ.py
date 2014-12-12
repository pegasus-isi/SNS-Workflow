#!/usr/bin/env python
import sys
import string
from ConfigParser import ConfigParser
from Pegasus.DAX3 import *

DAXGEN_DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DAXGEN_DIR, "templates")

def format_template(name, outfile, **kwargs):
    "This fills in the values for the template called 'name' and writes it to 'outfile'"
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
        "'outdir' is the directory where the workflow is written, and 'config' is a ConfigParser object"
        self.outdir = outdir
        self.config = config
        self.daxfile = os.path.join(self.outdir, "dax.xml")
        self.replicas = {}

        # Get all the values from the config file
        self.charges = [x.strip() for x in config.get("simulation", "charges").split(",")]
        self.temperature = config.get("simulation", "temperature")
        self.equilibrate_steps = config.get("simulation", "equilibrate_steps")
        self.production_steps = config.get("simulation", "production_steps")
        self.pressure = config.get("simulation", "pressure")
        #self.structure = config.get("simulation", "structure")
        self.coordinates = config.get("simulation", "coordinates")
        self.parameters = config.get("simulation", "parameters")
        self.topfile = config.get("simulation", "topfile")
        self.extended_system = config.get("simulation", "extended_system")
        self.sassena_db = config.get("simulation", "sassena_db")
        self.incoherent_db = "database/db-neutron-incoherent.xml"
        self.coherent_db = "database/db-neutron-coherent.xml"

    def add_replica(self, name, path):
        "Add a replica entry to the replica catalog for the workflow"
        url = "file://%s" % path
        self.replicas[name] = url

    def generate_replica_catalog(self):
        "Write the replica catalog for this workflow to a file"
        path = os.path.join(self.outdir, "rc.txt")
        f = open(path, "w")
        try:
            for name, url in self.replicas.items():
                f.write('%-30s %-100s pool="local"\n' % (name, url))
        finally:
            f.close()

    def generate_psf(self, charge):
        "Generate an psf files for charge'"
        name = "Q%s.psf" % charge
        path = os.path.join(self.outdir, name)
        kw = {
            "charge": str(0.01 * float(charge)),
            "charge2": str(-0.02 * float(charge)),
        }
        format_template("ce1.xml", path, **kw)
        self.add_replica(name, path)

    def generate_eq_conf(self, charge, structure):
        "Generate an equilibrate configuration file for 'charge'"
        name = "equilibrate_%s.conf" % charge
        path = os.path.join(self.outdir, name)
        kw = {
            "temperature": self.temperature,
            "pressure": self.pressure,
            "charge": charge,
            "structure": structure,
            "coordinates": self.coordinates,
            "parameters": self.parameters,
            "outputname": "equilibrate_%s" % charge,
            "extended_system": self.extended_system,
            "timesteps": self.equilibrate_steps
        }
        format_template("equilibrate.conf", path, **kw)
        self.add_replica(name, path)

    def generate_prod_conf(self, charge, structure):
        "Generate a production configuration file for 'charge'"
        name = "production_%s.conf" % charge
        path = os.path.join(self.outdir, name)
        kw = {
            "temperature": self.temperature,
            "pressure": self.pressure,
            "charge": charge,
            "structure": structure,
            "coordinates": self.coordinates,
            "parameters": self.parameters,
            "inputname": "equilibrate_%s" % charge,
            "outputname": "production_%s" % charge,
            "timesteps": self.production_steps
        }
        format_template("production.conf", path, **kw)
        self.add_replica(name, path)

    def generate_ptraj_conf(self, charge):
        "Generate a ptraj configuration file for 'charge'"
        name = "ptraj_%s.conf" % charge
        path = os.path.join(self.outdir, name)
        kw = {
            "trajectory_input": "production_%s.dcd" % charge,
            "trajectory_fit": "ptraj_%s.fit" % charge,
            "trajectory_output": "ptraj_%s.dcd" % charge
        }
        format_template("rms2first.ptraj", path, **kw)
        self.add_replica(name, path)

    def generate_incoherent_conf(self, charge):
        "Generate a sassena incoherent config file for 'charge'"
        name = "sassenaInc_%s.xml" % charge
        path = os.path.join(self.outdir, name)
        kw = {
            "coordinates": self.coordinates,
            "trajectory": "ptraj_%s.dcd" % charge,
            "output": "fqt_inc_%s.hd5" % charge,
            "database": self.incoherent_db
        }
        format_template("sassenaInc.xml", path, **kw)
        self.add_replica(name, path)

    def generate_coherent_conf(self, charge):
        "Generate a sassena coherent config file for 'charge'"
        name = "sassenaCoh_%s.xml" % charge
        path = os.path.join(self.outdir, name)
        kw = {
            "coordinates": self.coordinates,
            "trajectory": "ptraj_%s.dcd" % charge,
            "output": "fqt_coh_%s.hd5" % charge,
            "database": self.coherent_db
        }
        format_template("sassenaCoh.xml", path, **kw)
        self.add_replica(name, path)

    def generate_workflow(self):
        "Generate a workflow (DAX, config files, and replica catalog)"
        dax = ADAG("refinement")

        # These are all the global input files for the workflow
        #structure = File(self.structure)
        coordinates = File(self.coordinates)
        parameters = File(self.parameters)
        extended_system = File(self.extended_system)
        topfile = File(self.topfile)
        sassena_db = File(self.sassena_db)
        incoherent_db = File(self.incoherent_db)
        coherent_db = File(self.coherent_db)

        # This job untars the sassena db and makes it available to the other
        # jobs in the workflow
        untarjob = Job("tar", node_label="untar")
        untarjob.addArguments("-xzvf", sassena_db)
        untarjob.uses(sassena_db, link=Link.INPUT)
        untarjob.uses(incoherent_db, link=Link.OUTPUT, transfer=False)
        untarjob.uses(coherent_db, link=Link.OUTPUT, transfer=False)
        untarjob.profile("globus", "jobtype", "single")
        untarjob.profile("globus", "maxwalltime", "1")
        untarjob.profile("globus", "count", "1")
        dax.addJob(untarjob)

        # For each charge that was listed in the config file
        for charge in self.charges:

            structure = File("Q%s.psf" % charge)

            # Equilibrate files
            eq_conf = File("equilibrate_%s.conf" % charge)
            eq_coord = File("equilibrate_%s.restart.coord" % charge)
            eq_xsc = File("equilibrate_%s.restart.xsc" % charge)
            eq_vel = File("equilibrate_%s.restart.vel" % charge)

            # Production files
            prod_conf = File("production_%s.conf" % charge)
            prod_dcd = File("production_%s.dcd" % charge)

            # Ptraj files
            ptraj_conf = File("ptraj_%s.conf" % charge)
            ptraj_fit = File("ptraj_%s.fit" % charge)
            ptraj_dcd = File("ptraj_%s.dcd" % charge)

            # Sassena incoherent files
            incoherent_conf = File("sassenaInc_%s.xml" % charge)
            fqt_incoherent = File("fqt_inc_%s.hd5" % charge)

            # Sassena coherent files
            coherent_conf = File("sassenaCoh_%s.xml" % charge)
            fqt_coherent = File("fqt_coh_%s.hd5" % charge)

            # Generate psf and configuration files for this charge pipeline
            self.generate_psf(charge)
            self.generate_eq_conf(charge, structure)
            self.generate_prod_conf(charge, structure)
            self.generate_ptraj_conf(charge)
            self.generate_incoherent_conf(charge)
            self.generate_coherent_conf(charge)

            # Equilibrate job
            eqjob = Job("namd", node_label="namd_eq_%s" % charge)
            eqjob.addArguments(eq_conf)
            eqjob.uses(eq_conf, link=Link.INPUT)
            eqjob.uses(structure, link=Link.INPUT)
            eqjob.uses(coordinates, link=Link.INPUT)
            eqjob.uses(parameters, link=Link.INPUT)
            eqjob.uses(extended_system, link=Link.INPUT)
            eqjob.uses(eq_coord, link=Link.OUTPUT, transfer=False)
            eqjob.uses(eq_xsc, link=Link.OUTPUT, transfer=False)
            eqjob.uses(eq_vel, link=Link.OUTPUT, transfer=False)
            eqjob.profile("globus", "jobtype", "mpi")
            eqjob.profile("globus", "maxwalltime", "60")
            eqjob.profile("globus", "count", "288")
            dax.addJob(eqjob)

            # Production job
            prodjob = Job("namd", node_label="namd_prod_%s" % charge)
            prodjob.addArguments(prod_conf)
            prodjob.uses(prod_conf, link=Link.INPUT)
            prodjob.uses(structure, link=Link.INPUT)
            prodjob.uses(coordinates, link=Link.INPUT)
            prodjob.uses(parameters, link=Link.INPUT)
            prodjob.uses(eq_coord, link=Link.INPUT)
            prodjob.uses(eq_xsc, link=Link.INPUT)
            prodjob.uses(eq_vel, link=Link.INPUT)
            prodjob.uses(prod_dcd, link=Link.OUTPUT, transfer=True)
            prodjob.profile("globus", "jobtype", "mpi")
            prodjob.profile("globus", "maxwalltime", "360")
            prodjob.profile("globus", "count", "288")
            dax.addJob(prodjob)
            dax.depends(prodjob, eqjob)

            # ptraj job
            ptrajjob = Job(namespace="amber", name="ptraj", node_label="amber_ptraj_%s" % charge)
            ptrajjob.addArguments(topfile)
            ptrajjob.setStdin(ptraj_conf)
            ptrajjob.uses(topfile, link=Link.INPUT)
            ptrajjob.uses(ptraj_conf, link=Link.INPUT)
            ptrajjob.uses(prod_dcd, link=Link.INPUT)
            ptrajjob.uses(ptraj_fit, link=Link.OUTPUT, transfer=True)
            ptrajjob.uses(ptraj_dcd, link=Link.OUTPUT, transfer=True)
            ptrajjob.profile("globus", "jobtype", "single")
            ptrajjob.profile("globus", "maxwalltime", "60")
            ptrajjob.profile("globus", "count", "1")
            dax.addJob(ptrajjob)
            dax.depends(ptrajjob, prodjob)

            # sassena incoherent job
            incojob = Job("sassena", node_label="sassena_inc_%s" % charge)
            incojob.addArguments("--config", incoherent_conf)
            incojob.uses(incoherent_conf, link=Link.INPUT)
            incojob.uses(ptraj_dcd, link=Link.INPUT)
            incojob.uses(incoherent_db, link=Link.INPUT)
            incojob.uses(coordinates, link=Link.INPUT)
            incojob.uses(fqt_incoherent, link=Link.OUTPUT, transfer=True)
            incojob.profile("globus", "jobtype", "mpi")
            incojob.profile("globus", "maxwalltime", "360")
            incojob.profile("globus", "count", "144")
            dax.addJob(incojob)
            dax.depends(incojob, ptrajjob)
            dax.depends(incojob, untarjob)

            # sassena coherent job
            cojob = Job("sassena", node_label="sassena_coh_%s" % charge)
            cojob.addArguments("--config", coherent_conf)
            cojob.uses(coherent_conf, link=Link.INPUT)
            cojob.uses(ptraj_dcd, link=Link.INPUT)
            cojob.uses(coherent_db, link=Link.INPUT)
            cojob.uses(coordinates, link=Link.INPUT)
            cojob.uses(fqt_coherent, link=Link.OUTPUT, transfer=True)
            cojob.profile("globus", "jobtype", "mpi")
            cojob.profile("globus", "maxwalltime", "360")
            cojob.profile("globus", "count", "144")
            dax.addJob(cojob)
            dax.depends(cojob, prodjob)
            dax.depends(cojob, untarjob)

        # Write the DAX file
        dax.writeXMLFile(self.daxfile)

        # Finally, generate the replica catalog
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

    # Create the output directory
    outdir = os.path.abspath(outdir)
    os.makedirs(outdir)

    # Read the config file
    config = ConfigParser()
    config.read(configfile)

    # Generate the workflow in outdir based on the config file
    workflow = RefinementWorkflow(outdir, config)
    workflow.generate_workflow()


if __name__ == '__main__':
    main()


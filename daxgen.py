#!/usr/bin/env python
import sys
import string
import os
import shutil
from datetime import datetime
from ConfigParser import ConfigParser
from Pegasus.DAX3 import ADAG, Job, File, Link
from kegparametersfactory import KegParametersFactory

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
    def __init__(self, outdir, config, is_synthetic_workflow):
        "'outdir' is the directory where the workflow is written, and 'config' is a ConfigParser object"
        self.outdir = outdir
        self.config = config
        self.daxfile = os.path.join(self.outdir, "dax.xml")
        self.replicas = {}

        # Get all the values from the config file
        self.temperatures = [x.strip() for x in self.getconf("temperatures").split(",")]
        self.equilibrate_steps = self.getconf("equilibrate_steps")
        self.production_steps = self.getconf("production_steps")
        self.equilibrate_output = self.getconf("equilibrate_output")
        self.production_output = self.getconf("production_output")
        self.pressure = self.getconf("pressure")
        self.charge = self.getconf("charge")
        self.structure = self.getconf("structure")
        self.coordinates = self.getconf("coordinates")
        self.parameters = self.getconf("parameters")
        self.topfile = self.getconf("topfile")
        self.extended_system = self.getconf("extended_system")
        self.sassena_db = self.getconf("sassena_db")

        self.incoherent_db = "database/db-neutron-incoherent.xml"
        self.coherent_db = "database/db-neutron-coherent.xml"

        self.is_synthetic_workflow = is_synthetic_workflow
        # if synthetic workflow we do not have database dir
        if self.is_synthetic_workflow:
            self.incoherent_db = "db-neutron-incoherent.xml"
            self.coherent_db = "db-neutron-coherent.xml"
            self.keg_params = KegParametersFactory(self.config)

            # mocking input files
            for input_file in [ "structure", "coordinates", "parameters",
                "topfile", "extended_system", "sassena_db" ]:
                self.__dict__[input_file] = input_file + "_mock"
                mock_path = os.path.join("inputs", input_file + "_mock")
                self.keg_params.generate_input_file(input_file, mock_path)

    def getconf(self, name, section="simulation"):
        return self.config.get(section, name)

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

    def generate_eq_conf(self, temperature):
        "Generate an equilibrate configuration file for 'temperature'"
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
            "timesteps": self.equilibrate_steps,
            "timeoutput": self.equilibrate_output
        }
        format_template("equilibrate.conf", path, **kw)
        self.add_replica(name, path)

    def generate_prod_conf(self, temperature):
        "Generate a production configuration file for 'temperature'"
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
            "timesteps": self.production_steps,
            "timeoutput": self.production_output
        }
        format_template("production.conf", path, **kw)
        self.add_replica(name, path)

    def generate_ptraj_conf(self, temperature):
        "Generate a ptraj configuration file for 'temperature'"
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
        "Generate a sassena incoherent config file for 'temperature'"
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
        "Generate a sassena coherent config file for 'temperature'"
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

    def generate_dax(self):
        "Generate a workflow (DAX, config files, and replica catalog)"
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        dax = ADAG("refinement-%s" % ts)

        # These are all the global input files for the workflow
        structure = File(self.structure)
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

        if self.is_synthetic_workflow:
            untarjob.addArguments("-p", "-xzvf", sassena_db.name)
            untarjob.addArguments("-a", "tar")

            for output_file in [ "incoherent_db", "coherent_db" ]:
                untarjob.addArguments(self.keg_params.output_file("tar", output_file, eval(output_file).name))

            self.keg_params.add_keg_params(untarjob)
        else:
            untarjob.addArguments("-xzvf", sassena_db)

        untarjob.uses(sassena_db, link=Link.INPUT)
        untarjob.uses(incoherent_db, link=Link.OUTPUT, transfer=False)
        untarjob.uses(coherent_db, link=Link.OUTPUT, transfer=False)

        untarjob.profile("globus", "jobtype", "single")
        untarjob.profile("globus", "maxwalltime", "1")
        untarjob.profile("globus", "count", "1")

        dax.addJob(untarjob)

        # For each temperature that was listed in the config file
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

            # Generate configuration files for this temperature pipeline
            self.generate_eq_conf(temperature)
            self.generate_prod_conf(temperature)
            self.generate_ptraj_conf(temperature)
            self.generate_incoherent_conf(temperature)
            self.generate_coherent_conf(temperature)

            # Equilibrate job
            eqjob = Job("namd", node_label="namd_eq_%s" % temperature)
            if self.is_synthetic_workflow:
                eqjob.addArguments("-p", eq_conf)
                eqjob.addArguments("-a", "namd_eq_%s" % temperature)
                eqjob.addArguments("-i", eq_conf.name, structure.name, coordinates.name,
                    parameters.name, extended_system.name)

                task_label = "namd-eq"

                for output_file in [ "eq_coord", "eq_xsc", "eq_vel" ]:
                    eqjob.addArguments(self.keg_params.output_file(task_label, output_file, eval(output_file).name))

                self.keg_params.add_keg_params(eqjob, task_label)
            else:
                eqjob.addArguments(eq_conf)

            eqjob.uses(eq_conf, link=Link.INPUT)
            eqjob.uses(structure, link=Link.INPUT)
            eqjob.uses(coordinates, link=Link.INPUT)
            eqjob.uses(parameters, link=Link.INPUT)
            eqjob.uses(extended_system, link=Link.INPUT)
            eqjob.uses(eq_coord, link=Link.OUTPUT, transfer=False)
            eqjob.uses(eq_xsc, link=Link.OUTPUT, transfer=False)
            eqjob.uses(eq_vel, link=Link.OUTPUT, transfer=False)
            if self.is_synthetic_workflow:
                eqjob.profile("globus", "jobtype", "mpi")
                eqjob.profile("globus", "maxwalltime", "1")
                eqjob.profile("globus", "count", "8")
            else:
                eqjob.profile("globus", "jobtype", "mpi")
                eqjob.profile("globus", "maxwalltime", self.getconf("equilibrate_maxwalltime"))
                eqjob.profile("globus", "count", self.getconf("equilibrate_cores"))
            dax.addJob(eqjob)

            # Production job
            prodjob = Job("namd", node_label="namd_prod_%s" % temperature)

            if self.is_synthetic_workflow:
                prodjob.addArguments("-p", prod_conf)
                prodjob.addArguments("-a", "namd_prod_%s" % temperature)
                prodjob.addArguments("-i", prod_conf.name, structure.name, coordinates.name,
                    parameters.name, eq_coord.name, eq_xsc.name, eq_vel.name)

                task_label = "namd-prod"
                prodjob.addArguments(self.keg_params.output_file(task_label, "prod_dcd", prod_dcd.name))
                self.keg_params.add_keg_params(prodjob, task_label)
            else:
                prodjob.addArguments(prod_conf)

            prodjob.uses(prod_conf, link=Link.INPUT)
            prodjob.uses(structure, link=Link.INPUT)
            prodjob.uses(coordinates, link=Link.INPUT)
            prodjob.uses(parameters, link=Link.INPUT)
            prodjob.uses(eq_coord, link=Link.INPUT)
            prodjob.uses(eq_xsc, link=Link.INPUT)
            prodjob.uses(eq_vel, link=Link.INPUT)
            prodjob.uses(prod_dcd, link=Link.OUTPUT, transfer=True)

            if self.is_synthetic_workflow:
                prodjob.profile("globus", "jobtype", "mpi")
                prodjob.profile("globus", "maxwalltime", "6")
                prodjob.profile("globus", "count", "8")
            else:
                prodjob.profile("globus", "jobtype", "mpi")
                prodjob.profile("globus", "maxwalltime", self.getconf("production_maxwalltime"))
                prodjob.profile("globus", "count", self.getconf("production_cores"))

            dax.addJob(prodjob)
            dax.depends(prodjob, eqjob)

            # ptraj job
            ptrajjob = Job(namespace="amber", name="ptraj", node_label="amber_ptraj_%s" % temperature)

            if self.is_synthetic_workflow:
                ptrajjob.addArguments("-p", topfile)
                ptrajjob.addArguments("-a", "amber_ptraj_%s" % temperature)
                ptrajjob.addArguments("-i", topfile.name, ptraj_conf.name, prod_dcd.name)

                task_label = "amber-ptraj"

                for output_file in [ "ptraj_fit", "ptraj_dcd" ]:
                    ptrajjob.addArguments(self.keg_params.output_file(task_label, output_file, eval(output_file).name))

                self.keg_params.add_keg_params(ptrajjob, task_label)

            else:
                ptrajjob.addArguments(topfile)
                ptrajjob.setStdin(ptraj_conf)

            ptrajjob.uses(topfile, link=Link.INPUT)
            ptrajjob.uses(ptraj_conf, link=Link.INPUT)
            ptrajjob.uses(prod_dcd, link=Link.INPUT)
            ptrajjob.uses(ptraj_fit, link=Link.OUTPUT, transfer=True)
            ptrajjob.uses(ptraj_dcd, link=Link.OUTPUT, transfer=True)
            ptrajjob.profile("globus", "jobtype", "single")
            ptrajjob.profile("globus", "maxwalltime", self.getconf("ptraj_maxwalltime"))
            ptrajjob.profile("globus", "count", self.getconf("ptraj_cores"))
            dax.addJob(ptrajjob)
            dax.depends(ptrajjob, prodjob)

            # sassena incoherent job
            incojob = Job("sassena", node_label="sassena_inc_%s" % temperature)
            if self.is_synthetic_workflow:
                incojob.addArguments("-p", "--config", incoherent_conf)
                incojob.addArguments("-a", "sassena_inc_%s" % temperature)
                incojob.addArguments("-i", incoherent_conf.name, ptraj_dcd.name, incoherent_db.name, coordinates.name)

                task_label = "sassena-inc"

                incojob.addArguments(self.keg_params.output_file(task_label, "fqt_incoherent", fqt_incoherent.name))

                self.keg_params.add_keg_params(incojob, task_label)
            else:
                incojob.addArguments("--config", incoherent_conf)

            incojob.uses(incoherent_conf, link=Link.INPUT)
            incojob.uses(ptraj_dcd, link=Link.INPUT)
            incojob.uses(incoherent_db, link=Link.INPUT)
            incojob.uses(coordinates, link=Link.INPUT)
            incojob.uses(fqt_incoherent, link=Link.OUTPUT, transfer=True)

            if self.is_synthetic_workflow:
                incojob.profile("globus", "jobtype", "mpi")
                incojob.profile("globus", "maxwalltime", "6")
                incojob.profile("globus", "count", "8")
            else:
                incojob.profile("globus", "jobtype", "mpi")
                incojob.profile("globus", "maxwalltime", self.getconf("sassena_maxwalltime"))
                incojob.profile("globus", "count", self.getconf("sassena_cores"))

            dax.addJob(incojob)
            dax.depends(incojob, ptrajjob)
            dax.depends(incojob, untarjob)

            # sassena coherent job
            cojob = Job("sassena", node_label="sassena_coh_%s" % temperature)
            if self.is_synthetic_workflow:
                cojob.addArguments("-p", "--config", coherent_conf)
                cojob.addArguments("-a", "sassena_coh_%s" % temperature)
                cojob.addArguments("-i", coherent_conf.name, ptraj_dcd.name, coherent_db.name, coordinates.name)

                task_label = "sassena-coh"

                cojob.addArguments(self.keg_params.output_file(task_label, "fqt_coherent", fqt_coherent.name))

                self.keg_params.add_keg_params(cojob, task_label)

            else:
                cojob.addArguments("--config", coherent_conf)

            cojob.uses(coherent_conf, link=Link.INPUT)
            cojob.uses(ptraj_dcd, link=Link.INPUT)
            cojob.uses(coherent_db, link=Link.INPUT)
            cojob.uses(coordinates, link=Link.INPUT)
            cojob.uses(fqt_coherent, link=Link.OUTPUT, transfer=True)

            if self.is_synthetic_workflow:
                cojob.profile("globus", "jobtype", "mpi")
                cojob.profile("globus", "maxwalltime", "6")
                cojob.profile("globus", "count", "8")
            else:
                cojob.profile("globus", "jobtype", "mpi")
                cojob.profile("globus", "maxwalltime", self.getconf("sassena_maxwalltime"))
                cojob.profile("globus", "count", self.getconf("sassena_cores"))

            dax.addJob(cojob)
            dax.depends(cojob, prodjob)
            dax.depends(cojob, untarjob)

        # Write the DAX file
        dax.writeXMLFile(self.daxfile)

    def generate_workflow(self):

        # Generate dax
        self.generate_dax()

        # Generate the replica catalog
        self.generate_replica_catalog()

def main():
    if len(sys.argv) < 3:
        raise Exception("Usage: %s --synthetic CONFIGFILE OUTDIR" % sys.argv[0])

    is_synthetic_workflow = (sys.argv[1] == "--synthetic")

    if is_synthetic_workflow:
        configfile = sys.argv[2]
        outdir = sys.argv[3]
    else:
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

    # Save a copy of the config file
    shutil.copy(configfile, outdir)

    # Generate the workflow in outdir based on the config file
    workflow = RefinementWorkflow(outdir, config, is_synthetic_workflow)
    workflow.generate_workflow()


if __name__ == '__main__':
    main()


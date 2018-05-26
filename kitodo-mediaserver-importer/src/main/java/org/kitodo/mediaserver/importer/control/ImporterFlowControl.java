/*
 * (c) Kitodo. Key to digital objects e. V. <contact@kitodo.org>
 *
 * This file is part of the Kitodo project.
 *
 * It is licensed under GNU General Public License version 3 or later.
 *
 * For the full copyright and license information, please read the
 * LICENSE file that was distributed with this source code.
 */

package org.kitodo.mediaserver.importer.control;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.kitodo.mediaserver.core.api.IDataReader;
import org.kitodo.mediaserver.core.config.FileserverProperties;
import org.kitodo.mediaserver.core.db.entities.Work;
import org.kitodo.mediaserver.core.exceptions.ValidationException;
import org.kitodo.mediaserver.core.services.WorkService;
import org.kitodo.mediaserver.core.util.FileDeleter;
import org.kitodo.mediaserver.importer.api.IImportValidation;
import org.kitodo.mediaserver.importer.api.IMetsValidation;
import org.kitodo.mediaserver.importer.api.IWorkChecker;
import org.kitodo.mediaserver.importer.config.ImporterProperties;
import org.kitodo.mediaserver.importer.exceptions.ImporterException;
import org.kitodo.mediaserver.importer.util.ImporterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Flow control class for the importer.
 */
@Component
public class ImporterFlowControl implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImporterFlowControl.class);

    private ImporterUtils importerUtils;
    private ImporterProperties importerProperties;
    private FileserverProperties fileserverProperties;
    private IImportValidation importValidation;
    private IDataReader workDataReader;
    private IWorkChecker workChecker;
    private WorkService workService;
    private FileDeleter fileDeleter;

    @Autowired
    public void setImporterUtils(ImporterUtils importerUtils) {
        this.importerUtils = importerUtils;
    }

    @Autowired
    public void setImporterProperties(ImporterProperties importerProperties) {
        this.importerProperties = importerProperties;
    }

    @Autowired
    public void setFileserverProperties(FileserverProperties fileserverProperties) {
        this.fileserverProperties = fileserverProperties;
    }

    @Autowired
    public void setImportValidation(IImportValidation importValidation) {
        this.importValidation = importValidation;
    }

    @Autowired
    public void setWorkDataReader(IDataReader workDataReader) {
        this.workDataReader = workDataReader;
    }

    @Autowired
    public void setWorkChecker(IWorkChecker workChecker) {
        this.workChecker = workChecker;
    }

    @Autowired
    public void setWorkService(WorkService workService) {
        this.workService = workService;
    }

    @Autowired
    public void setFileDeleter(FileDeleter fileDeleter) {
        this.fileDeleter = fileDeleter;
    }

    /**
     * Runs the importer algorithm.
     *
     * @param args cli arguments
     * @throws Exception if a severe error occurs
     */
    @Override
    public void run(String... args) throws Exception {

        importScheduled();

    }

    /**
     * Controls the importer algorithm.
     * TODO this is just a stub - work in progress
     *
     * @throws Exception if a severe error occurs
     */
    /* @Scheduled(fixedRate = 1000) */
    private void importScheduled() throws Exception {

        // Get a work from the hotfolder and move it to the import-in-progress-folder. Make sure, this set of files is
        // in a subdirectory named as the XML file with the mets-mods-data.

        File workDir;

        while ((workDir = importerUtils.getWorkPackage()) != null) {

            LOGGER.info("Starting import of work " + workDir.getName());

            Path tempOldWorkFiles = null;
            Work presentWork = null;
            Work newWork = null;

            try {

                // Get the mets file
                File mets = new File(workDir, workDir.getName() + ".xml");
                if (!mets.exists()) {
                    throw new ImporterException("Mets file not found, expected at " + mets.getAbsolutePath());
                }

                // * Read the work data from the mets-mods file.
                newWork = workDataReader.read(mets);
                newWork.setPath(Paths.get(importerProperties.getWorkFilesPath(), newWork.getId()).toString());

                // Validate import data
                importValidation.validate(newWork, mets);

                //check that naming of folder and mets.xml concedes with workId, otherwise rename
                if (!StringUtils.equals(newWork.getId(), workDir.getName())) {
                    LOGGER.info("Id of work to import: " + newWork.getId() + " is different from the mets file name "
                            + workDir.getName() + ", renaming.");

                    Files.move(mets.toPath(), Paths.get(mets.getParent(), newWork.getId() + ".xml"));
                    Path newPath = Paths.get(workDir.getParent(), newWork.getId());
                    Files.move(workDir.toPath(), newPath);
                    workDir = newPath.toFile();
                }

                // Check in the database if this work is already present
                // and if there are identifiers associated to another work.
                presentWork = workChecker.check(newWork);

                // If the work is already present, it should be replaced.
                if (presentWork != null) {

                    LOGGER.info("Work " + newWork.getId() + " already present, replacing");

                    newWork.setEnabled(presentWork.isEnabled());

                    // Files created and cached by the fileserver must be deleted. TODO call action instead?
                    fileDeleter.delete(Paths.get(fileserverProperties.getCachePath(), newWork.getId()));

                    // Move old work files to a temporary folder.
                    // TODO use importerUtils?
                    tempOldWorkFiles = Paths.get(importerProperties.getTempWorkFolderPath(), presentWork.getId());
                    Files.move(
                            Paths.get(presentWork.getPath()),
                            tempOldWorkFiles
                    );
                }

                // Move work files to the production root.
                Files.move(
                        workDir.toPath(),
                        Paths.get(newWork.getPath())
                );

                // * Perform all defined direct import actions (doi registration…).
                //
                // * Order all defined asynchronous import actions (creation of additional files…).
                //

                // Insert the work data into the database, updating if old data present.
                workService.importWork(newWork);

                // Delete temporary files.
                if (tempOldWorkFiles != null) {
                    fileDeleter.delete(tempOldWorkFiles);
                }

                // * Trigger indexing in presentation system (configurable).

                LOGGER.info("Finished import of work " + workDir.getName());

            } catch (Exception e) {
                LOGGER.error("An error occured importing work " + workDir.getName()
                        + ", moving all files to the error folder. Error: " + e);

                // TODO this rollback has to be consolidated

                if (presentWork != null) {
                    // restore old work in database
                    workService.importWork(presentWork);

                    // restore old work files
                    if (tempOldWorkFiles != null) {
                        Files.move(
                                tempOldWorkFiles,
                                Paths.get(presentWork.getPath())
                        );
                    }
                }

                // move import files to error folder
                importerUtils.moveDir(workDir, new File(importerProperties.getErrorFolderPath()));

            }

        }


        // If a severe error occures during the process, the following steps must be executed:
        //
        // * Move work folder to the error folder.
        //
        // * If there were old data for this work present:
        //
        // * Move old work files from the temporary folder back to the original folder.
        //
        // * If the error occured writing to the database, rollback (of course).
    }
}

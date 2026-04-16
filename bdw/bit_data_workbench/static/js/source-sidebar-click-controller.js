export function createSourceSidebarClickController(helpers) {
  const {
    closeResultActionMenus,
    closeS3ExplorerActionMenus,
    closeSourceActionMenus,
    cleanupDataGenerationJob,
    cancelDataGenerationJob,
    cancelQueryJob,
    createLocalWorkspaceFolder,
    createLocalWorkspaceFolderFromDialog,
    createLocalWorkspaceFolderFromMoveDialog,
    createS3ExplorerBucket,
    createS3ExplorerFolder,
    createSidebarS3Bucket,
    deleteLocalWorkspaceExportFromSource,
    deleteLocalWorkspaceFolder,
    deleteS3EntryDescriptor,
    deleteS3ExplorerEntry,
    downloadLocalWorkspaceExportFromSource,
    downloadQueryResultExport,
    downloadS3ExplorerObject,
    downloadSourceS3Object,
    loadS3ExplorerNode,
    openLocalWorkspaceMoveDialog,
    openLocalWorkspaceSaveDialog,
    openNotebookForQueryJob,
    openResultDownloadDialog,
    openResultExportDialog,
    queryJobForResultActionTarget,
    queryNotificationMenu,
    querySourceInCurrentNotebook,
    querySourceInNewNotebook,
    revealS3ExplorerLocation,
    selectResultExportLocation,
    selectSourceObject,
    setDataSourceConnectionState,
    setDataSourceTreeExpanded,
    setNotebookTreeExpanded,
    setRunbookTreeExpanded,
    showConfirmDialog,
    showMessageDialog,
    sourceObjectS3DeleteDescriptor,
    sourceSchemaS3BucketDescriptor,
    startDataGenerationJob,
    syncSourceActionMenu,
    updateLocalWorkspaceMoveFolderPath,
    updateLocalWorkspaceSaveFolderPath,
    viewSourceData,
  } = helpers;

  async function handleClick(event) {
    const sourceActionMenu = event.target.closest("[data-source-action-menu]");
    if (sourceActionMenu) {
      syncSourceActionMenu(sourceActionMenu);
    }

    const sourceActionMenuToggle = event.target.closest("[data-source-action-menu-toggle]");
    if (sourceActionMenuToggle) {
      event.preventDefault();
      event.stopPropagation();
      const menu = sourceActionMenuToggle.closest("[data-source-action-menu]");
      if (menu instanceof HTMLDetailsElement) {
        const nextOpen = !menu.open;
        closeSourceActionMenus(nextOpen ? menu : null);
        menu.open = nextOpen;
        syncSourceActionMenu(menu);
      }
      return true;
    }

    const createSourceBucketButton = event.target.closest("[data-create-source-bucket]");
    if (createSourceBucketButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();
      try {
        await createSidebarS3Bucket();
      } catch (error) {
        console.error("Failed to create the sidebar S3 bucket.", error);
        await showMessageDialog({
          title: "S3 bucket creation failed",
          copy: error instanceof Error ? error.message : "The S3 bucket could not be created.",
        });
      }
      return true;
    }

    const createLocalWorkspaceRootFolderButton = event.target.closest(
      "[data-create-local-workspace-root-folder]"
    );
    if (createLocalWorkspaceRootFolderButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();
      try {
        await createLocalWorkspaceFolder("", {
          confirmCreation: true,
          showSidebarStatus: true,
          revealSidebar: true,
        });
      } catch (error) {
        console.error("Failed to create the Local Workspace folder.", error);
        await showMessageDialog({
          title: "Local Workspace folder creation failed",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace folder could not be created.",
        });
      }
      return true;
    }

    const cancelQueryButton = event.target.closest("[data-cancel-query]");
    if (cancelQueryButton) {
      event.preventDefault();
      await cancelQueryJob(cancelQueryButton.dataset.jobId || "");
      return true;
    }

    const cancelQueryJobButton = event.target.closest("[data-cancel-query-job]");
    if (cancelQueryJobButton) {
      event.preventDefault();
      await cancelQueryJob(cancelQueryJobButton.dataset.cancelQueryJob || "");
      return true;
    }

    const cancelDataGenerationButton = event.target.closest("[data-cancel-data-generation-job]");
    if (cancelDataGenerationButton) {
      event.preventDefault();
      await cancelDataGenerationJob(cancelDataGenerationButton.dataset.cancelDataGenerationJob || "");
      return true;
    }

    const cleanupDataGenerationButton = event.target.closest("[data-cleanup-data-generation-job]");
    if (cleanupDataGenerationButton) {
      event.preventDefault();
      const jobCard = cleanupDataGenerationButton.closest("[data-data-generation-job-card]");
      const jobTitle =
        jobCard?.querySelector(".ingestion-job-copy h4")?.textContent?.trim() || "Generated data";
      const { confirmed } = await showConfirmDialog({
        title: "Clean loader data",
        copy: `Clean the generated loader data for ${jobTitle}? This keeps the target structure but removes the loaded data.`,
        confirmLabel: "Clean loader data",
      });
      if (!confirmed) {
        return true;
      }
      await cleanupDataGenerationJob(cleanupDataGenerationButton.dataset.cleanupDataGenerationJob || "");
      return true;
    }

    const startDataGenerationButton = event.target.closest("[data-start-data-generation]");
    if (startDataGenerationButton) {
      event.preventDefault();
      const generatorCard = startDataGenerationButton.closest("[data-generator-card]");
      const sizeInput = generatorCard?.querySelector("[data-ingestion-size-input]");
      const requestedSize = Number(sizeInput?.value ?? 0);
      await startDataGenerationJob(
        startDataGenerationButton.dataset.startDataGeneration || "",
        requestedSize
      );
      return true;
    }

    const openQueryNotebookButton = event.target.closest("[data-open-query-notebook]");
    if (openQueryNotebookButton) {
      event.preventDefault();
      queryNotificationMenu()?.removeAttribute("open");
      await openNotebookForQueryJob(
        openQueryNotebookButton.dataset.openQueryNotebook || "",
        openQueryNotebookButton.dataset.openQueryCell || ""
      );
      return true;
    }

    const downloadResultExportButton = event.target.closest("[data-result-export-download]");
    if (downloadResultExportButton) {
      event.preventDefault();
      closeResultActionMenus();
      const job = queryJobForResultActionTarget(downloadResultExportButton);
      if (!job) {
        await showMessageDialog({
          title: "Result export unavailable",
          copy: "Run the cell again so the current query result can be exported.",
        });
        return true;
      }
      try {
        await openResultDownloadDialog(job);
      } catch (error) {
        console.error("Failed to download the query result export.", error);
        await showMessageDialog({
          title: "Result export failed",
          copy: error instanceof Error ? error.message : "The query result could not be downloaded.",
        });
      }
      return true;
    }

    const saveResultExportButton = event.target.closest("[data-result-export-s3]");
    if (saveResultExportButton) {
      event.preventDefault();
      closeResultActionMenus();
      const job = queryJobForResultActionTarget(saveResultExportButton);
      if (!job) {
        await showMessageDialog({
          title: "Result export unavailable",
          copy: "Run the cell again so the current query result can be saved to Shared Workspace.",
        });
        return true;
      }
      try {
        await openResultExportDialog(job);
      } catch (error) {
        console.error("Failed to open the result export dialog.", error);
        await showMessageDialog({
          title: "Result export failed",
          copy: error instanceof Error ? error.message : "The query result export dialog could not be opened.",
        });
      }
      return true;
    }

    const saveLocalResultExportButton = event.target.closest("[data-result-export-local]");
    if (saveLocalResultExportButton) {
      event.preventDefault();
      closeResultActionMenus();
      const job = queryJobForResultActionTarget(saveLocalResultExportButton);
      if (!job) {
        await showMessageDialog({
          title: "Result export unavailable",
          copy: "Run the cell again so the current query result can be saved to Local Workspace (IndexDB).",
        });
        return true;
      }
      try {
        await openLocalWorkspaceSaveDialog(job);
      } catch (error) {
        console.error("Failed to open the Local Workspace save dialog.", error);
        await showMessageDialog({
          title: "Local Workspace (IndexDB) save unavailable",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace (IndexDB) save dialog could not be opened.",
        });
      }
      return true;
    }

    const createLocalWorkspaceFolderButton = event.target.closest(
      "[data-local-workspace-create-folder]"
    );
    if (createLocalWorkspaceFolderButton) {
      event.preventDefault();
      try {
        await createLocalWorkspaceFolderFromDialog();
      } catch (error) {
        console.error("Failed to create a Local Workspace folder.", error);
        await showMessageDialog({
          title: "Local Workspace folder error",
          copy: error instanceof Error ? error.message : "The Local Workspace folder could not be created.",
        });
      }
      return true;
    }

    const createLocalWorkspaceMoveFolderButton = event.target.closest(
      "[data-local-workspace-move-create-folder]"
    );
    if (createLocalWorkspaceMoveFolderButton) {
      event.preventDefault();
      try {
        await createLocalWorkspaceFolderFromMoveDialog();
      } catch (error) {
        console.error("Failed to create a Local Workspace move target folder.", error);
        await showMessageDialog({
          title: "Local Workspace folder error",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace folder could not be created.",
        });
      }
      return true;
    }

    const localWorkspaceFolderOptionButton = event.target.closest(
      "[data-local-workspace-folder-option]"
    );
    if (localWorkspaceFolderOptionButton) {
      event.preventDefault();
      updateLocalWorkspaceSaveFolderPath(
        localWorkspaceFolderOptionButton.dataset.localWorkspaceFolderPath || ""
      );
      return true;
    }

    const localWorkspaceMoveFolderOptionButton = event.target.closest(
      "[data-local-workspace-move-folder-option]"
    );
    if (localWorkspaceMoveFolderOptionButton) {
      event.preventDefault();
      updateLocalWorkspaceMoveFolderPath(
        localWorkspaceMoveFolderOptionButton.dataset.localWorkspaceFolderPath || ""
      );
      return true;
    }

    const localWorkspaceBreadcrumbButton = event.target.closest(
      "[data-local-workspace-breadcrumb]"
    );
    if (localWorkspaceBreadcrumbButton) {
      event.preventDefault();
      updateLocalWorkspaceSaveFolderPath(
        localWorkspaceBreadcrumbButton.dataset.localWorkspaceFolderPath || ""
      );
      return true;
    }

    const localWorkspaceMoveBreadcrumbButton = event.target.closest(
      "[data-local-workspace-move-breadcrumb]"
    );
    if (localWorkspaceMoveBreadcrumbButton) {
      event.preventDefault();
      updateLocalWorkspaceMoveFolderPath(
        localWorkspaceMoveBreadcrumbButton.dataset.localWorkspaceFolderPath || ""
      );
      return true;
    }

    const createS3BucketButton = event.target.closest("[data-s3-create-bucket]");
    if (createS3BucketButton) {
      event.preventDefault();
      try {
        await createS3ExplorerBucket();
      } catch (error) {
        console.error("Failed to create the S3 bucket.", error);
        await showMessageDialog({
          title: "S3 location error",
          copy: error instanceof Error ? error.message : "The bucket could not be created.",
        });
      }
      return true;
    }

    const createS3FolderButton = event.target.closest("[data-s3-create-folder]");
    if (createS3FolderButton) {
      event.preventDefault();
      try {
        await createS3ExplorerFolder();
      } catch (error) {
        console.error("Failed to create the S3 folder.", error);
        await showMessageDialog({
          title: "S3 location error",
          copy: error instanceof Error ? error.message : "The folder could not be created.",
        });
      }
      return true;
    }

    const s3ExplorerActionMenuToggle = event.target.closest("[data-s3-explorer-action-menu-toggle]");
    if (s3ExplorerActionMenuToggle) {
      event.preventDefault();
      event.stopPropagation();
      const menu = s3ExplorerActionMenuToggle.closest("[data-s3-explorer-action-menu]");
      if (menu instanceof HTMLDetailsElement) {
        const nextOpen = !menu.open;
        closeS3ExplorerActionMenus(nextOpen ? menu : null);
        menu.open = nextOpen;
      }
      return true;
    }

    const s3ExplorerEntryDownloadButton = event.target.closest("[data-s3-explorer-entry-download]");
    if (s3ExplorerEntryDownloadButton) {
      event.preventDefault();
      event.stopPropagation();
      closeS3ExplorerActionMenus();
      const downloaded = downloadS3ExplorerObject(s3ExplorerEntryDownloadButton);
      if (downloaded === false) {
        await showMessageDialog({
          title: "S3 download unavailable",
          copy: "This S3 entry does not point to a single downloadable object.",
        });
      }
      return true;
    }

    const s3ExplorerEntryDeleteButton = event.target.closest("[data-s3-explorer-entry-delete]");
    if (s3ExplorerEntryDeleteButton) {
      event.preventDefault();
      event.stopPropagation();
      closeS3ExplorerActionMenus();
      try {
        await deleteS3ExplorerEntry(s3ExplorerEntryDeleteButton);
      } catch (error) {
        console.error("Failed to delete the S3 explorer entry.", error);
        await showMessageDialog({
          title: "S3 delete failed",
          copy: error instanceof Error ? error.message : "The selected S3 entry could not be deleted.",
        });
      }
      return true;
    }

    const s3ExplorerBreadcrumbButton = event.target.closest("[data-s3-explorer-breadcrumb]");
    if (s3ExplorerBreadcrumbButton) {
      event.preventDefault();
      try {
        await revealS3ExplorerLocation(
          s3ExplorerBreadcrumbButton.dataset.s3BreadcrumbBucket || "",
          s3ExplorerBreadcrumbButton.dataset.s3BreadcrumbPrefix || ""
        );
      } catch (error) {
        console.error("Failed to navigate the S3 explorer.", error);
        await showMessageDialog({
          title: "S3 explorer error",
          copy: error instanceof Error ? error.message : "The selected S3 location could not be opened.",
        });
      }
      return true;
    }

    const s3ExplorerNodeSummary = event.target.closest(".s3-explorer-node-summary");
    if (s3ExplorerNodeSummary) {
      const node = s3ExplorerNodeSummary.closest("[data-s3-explorer-node]");
      if (node instanceof HTMLElement) {
        selectResultExportLocation(node.dataset.s3ExplorerBucket || "", node.dataset.s3ExplorerPrefix || "");
        window.setTimeout(() => {
          if (!node.open) {
            return;
          }
          loadS3ExplorerNode(node).catch(async (error) => {
            console.error("Failed to expand the S3 explorer node.", error);
            await showMessageDialog({
              title: "S3 explorer error",
              copy: error instanceof Error ? error.message : "The S3 location could not be loaded.",
            });
          });
        }, 0);
      }
      return true;
    }

    const collapseTreeButton = event.target.closest("[data-collapse-tree]");
    if (collapseTreeButton) {
      event.preventDefault();
      event.stopPropagation();
      setNotebookTreeExpanded(false);
      return true;
    }

    const collapseRunbooksButton = event.target.closest("[data-collapse-runbooks]");
    if (collapseRunbooksButton) {
      event.preventDefault();
      event.stopPropagation();
      setRunbookTreeExpanded(false);
      return true;
    }

    const expandTreeButton = event.target.closest("[data-expand-tree]");
    if (expandTreeButton) {
      event.preventDefault();
      event.stopPropagation();
      setNotebookTreeExpanded(true);
      return true;
    }

    const expandRunbooksButton = event.target.closest("[data-expand-runbooks]");
    if (expandRunbooksButton) {
      event.preventDefault();
      event.stopPropagation();
      setRunbookTreeExpanded(true);
      return true;
    }

    const collapseSourcesButton = event.target.closest("[data-collapse-sources]");
    if (collapseSourcesButton) {
      event.preventDefault();
      event.stopPropagation();
      setDataSourceTreeExpanded(false);
      return true;
    }

    const expandSourcesButton = event.target.closest("[data-expand-sources]");
    if (expandSourcesButton) {
      event.preventDefault();
      event.stopPropagation();
      setDataSourceTreeExpanded(true);
      return true;
    }

    const sourceConnectButton = event.target.closest("[data-source-connect]");
    if (sourceConnectButton) {
      event.preventDefault();
      event.stopPropagation();
      await setDataSourceConnectionState(sourceConnectButton.dataset.sourceConnect, "connect");
      return true;
    }

    const sourceDisconnectButton = event.target.closest("[data-source-disconnect]");
    if (sourceDisconnectButton) {
      event.preventDefault();
      event.stopPropagation();
      await setDataSourceConnectionState(sourceDisconnectButton.dataset.sourceDisconnect, "disconnect");
      return true;
    }

    const sourceObjectRoot = event.target.closest("[data-source-object]");
    if (sourceObjectRoot && !event.target.closest("[data-source-action-menu]")) {
      try {
        await selectSourceObject(sourceObjectRoot);
      } catch (error) {
        console.error("Failed to load source object fields.", error);
      }
      return true;
    }

    const querySourceCurrentButton = event.target.closest("[data-query-source-current]");
    if (querySourceCurrentButton) {
      event.preventDefault();
      closeSourceActionMenus();

      const sourceObjectRoot = querySourceCurrentButton.closest("[data-source-object]");
      const inserted = await querySourceInCurrentNotebook(sourceObjectRoot);
      if (inserted === false) {
        window.alert("Open an editable notebook first, or use 'Query in new notebook'.");
      }
      return true;
    }

    const downloadSourceS3ObjectButton = event.target.closest("[data-download-source-s3-object]");
    if (downloadSourceS3ObjectButton) {
      event.preventDefault();
      closeSourceActionMenus();

      const downloaded = downloadSourceS3Object(downloadSourceS3ObjectButton.closest("[data-source-object]"));
      if (downloaded === false) {
        await showMessageDialog({
          title: "S3 download unavailable",
          copy: "This source object does not point to a single downloadable S3 object.",
        });
      }
      return true;
    }

    const downloadLocalWorkspaceObjectButton = event.target.closest(
      "[data-download-local-workspace-object]"
    );
    if (downloadLocalWorkspaceObjectButton) {
      event.preventDefault();
      closeSourceActionMenus();

      const downloaded = await downloadLocalWorkspaceExportFromSource(
        downloadLocalWorkspaceObjectButton.closest("[data-source-object]")
      );
      if (downloaded === false) {
        await showMessageDialog({
          title: "Local Workspace download unavailable",
          copy: "This Local Workspace file could not be downloaded from browser storage.",
        });
      }
      return true;
    }

    const moveLocalWorkspaceObjectButton = event.target.closest(
      "[data-move-local-workspace-object]"
    );
    if (moveLocalWorkspaceObjectButton) {
      event.preventDefault();
      closeSourceActionMenus();

      const opened = await openLocalWorkspaceMoveDialog(
        moveLocalWorkspaceObjectButton.closest("[data-source-object]")
      );
      if (opened === false) {
        await showMessageDialog({
          title: "Local Workspace move unavailable",
          copy: "This Local Workspace file could not be loaded for moving.",
        });
      }
      return true;
    }

    const deleteSourceS3ObjectButton = event.target.closest("[data-delete-source-s3-object]");
    if (deleteSourceS3ObjectButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();

      const descriptor = sourceObjectS3DeleteDescriptor(
        deleteSourceS3ObjectButton.closest("[data-source-object]")
      );
      if (!descriptor) {
        await showMessageDialog({
          title: "S3 delete unavailable",
          copy: "This source object does not point to a single deletable S3 object.",
        });
        return true;
      }

      try {
        await deleteS3EntryDescriptor(descriptor, { refreshSidebarAfter: true, showSidebarStatus: true });
      } catch (error) {
        console.error("Failed to delete the sidebar S3 object.", error);
        await showMessageDialog({
          title: "S3 delete failed",
          copy: error instanceof Error ? error.message : "The selected S3 object could not be deleted.",
        });
      }
      return true;
    }

    const deleteLocalWorkspaceObjectButton = event.target.closest(
      "[data-delete-local-workspace-object]"
    );
    if (deleteLocalWorkspaceObjectButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();

      const deleted = await deleteLocalWorkspaceExportFromSource(
        deleteLocalWorkspaceObjectButton.closest("[data-source-object]")
      );
      if (deleted === false) {
        await showMessageDialog({
          title: "Local Workspace delete unavailable",
          copy: "This Local Workspace file could not be deleted from browser storage.",
        });
      }
      return true;
    }

    const createLocalWorkspaceSidebarFolderButton = event.target.closest(
      "[data-create-local-workspace-folder-path]"
    );
    if (createLocalWorkspaceSidebarFolderButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();

      try {
        await createLocalWorkspaceFolder(
          createLocalWorkspaceSidebarFolderButton.dataset.createLocalWorkspaceFolderPath || "",
          {
            confirmCreation: true,
            showSidebarStatus: true,
            revealSidebar: true,
          }
        );
      } catch (error) {
        console.error("Failed to create the Local Workspace sidebar folder.", error);
        await showMessageDialog({
          title: "Local Workspace folder creation failed",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace folder could not be created.",
        });
      }
      return true;
    }

    const deleteLocalWorkspaceFolderButton = event.target.closest(
      "[data-delete-local-workspace-folder-path]"
    );
    if (deleteLocalWorkspaceFolderButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();

      try {
        await deleteLocalWorkspaceFolder(
          deleteLocalWorkspaceFolderButton.dataset.deleteLocalWorkspaceFolderPath || ""
        );
      } catch (error) {
        console.error("Failed to delete the Local Workspace sidebar folder.", error);
        await showMessageDialog({
          title: "Local Workspace folder delete failed",
          copy:
            error instanceof Error
              ? error.message
              : "The Local Workspace folder could not be deleted.",
        });
      }
      return true;
    }

    const deleteSourceS3BucketButton = event.target.closest("[data-delete-source-s3-bucket]");
    if (deleteSourceS3BucketButton) {
      event.preventDefault();
      event.stopPropagation();
      closeSourceActionMenus();

      const descriptor = sourceSchemaS3BucketDescriptor(
        deleteSourceS3BucketButton.closest("[data-source-schema]")
      );
      if (!descriptor) {
        await showMessageDialog({
          title: "Bucket delete unavailable",
          copy: "This source entry does not point to a deletable S3 bucket.",
        });
        return true;
      }

      try {
        await deleteS3EntryDescriptor(descriptor, { refreshSidebarAfter: true, showSidebarStatus: true });
      } catch (error) {
        console.error("Failed to delete the sidebar S3 bucket.", error);
        await showMessageDialog({
          title: "Bucket delete failed",
          copy: error instanceof Error ? error.message : "The selected bucket could not be deleted.",
        });
      }
      return true;
    }

    const viewSourceDataButton = event.target.closest("[data-view-source-data]");
    if (viewSourceDataButton) {
      event.preventDefault();
      closeSourceActionMenus();

      const sourceObjectRoot = viewSourceDataButton.closest("[data-source-object]");
      const viewed = await viewSourceData(sourceObjectRoot);
      if (viewed === false) {
        window.alert("Open an editable notebook first, or use 'Query in new notebook'.");
      }
      return true;
    }

    const querySourceNewButton = event.target.closest("[data-query-source-new]");
    if (querySourceNewButton) {
      event.preventDefault();
      closeSourceActionMenus();
      await querySourceInNewNotebook(querySourceNewButton.closest("[data-source-object]"));
      return true;
    }

    return false;
  }

  return {
    handleClick,
  };
}

using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Web;

namespace FileCustomAPIs.Plugins
{
    public class CopyFileColumnToFileColumn : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            if (context.MessageName.Equals("fca_CopyFileColumnToFileColumn") && context.Stage.Equals(30))
            {
                try
                {
                    // Input Parameters
                    string sourceTable = context.InputParameters["SourceTable"] as string;
                    string sourceId = context.InputParameters["SourceId"] as string;
                    string sourceColumn = context.InputParameters["SourceColumn"] as string;
                    string targetTable = context.InputParameters["TargetTable"] as string;
                    string targetId = context.InputParameters["TargetId"] as string;
                    string targetColumn = context.InputParameters["TargetColumn"] as string;
                    string newFilename = context.InputParameters["NewFilename"] as string;

                    // Default Output Parameters
                    bool success = false;
                    string error = "";

                    try
                    {
                        IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                        IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

                        Entity sourceEntity = service.Retrieve(sourceTable, new Guid(sourceId), new ColumnSet(sourceColumn, $"{sourceColumn}_name"));

                        Guid sourceColumnFileId = sourceEntity.GetAttributeValue<Guid>(sourceColumn);
                        if (sourceColumnFileId != Guid.Empty)
                        {
                            // Get Filename
                            string targetFilename = sourceEntity.GetAttributeValue<string>($"{sourceColumn}_name");
                            if (!string.IsNullOrWhiteSpace(newFilename)) { targetFilename = newFilename; }

                            // Get File Content
                            InitializeFileBlocksDownloadRequest initializeFileBlocksDownloadRequest = new InitializeFileBlocksDownloadRequest
                            {
                                Target = new EntityReference(sourceTable, new Guid(sourceId)),
                                FileAttributeName = sourceColumn
                            };

                            InitializeFileBlocksDownloadResponse initializeFileBlocksDownloadResponse = (InitializeFileBlocksDownloadResponse)service.Execute(initializeFileBlocksDownloadRequest);

                            string fileContinuationTokenDownload = initializeFileBlocksDownloadResponse.FileContinuationToken;
                            long fileSizeInBytes = initializeFileBlocksDownloadResponse.FileSizeInBytes;
                            List<byte> fileBytes = new List<byte>((int)fileSizeInBytes);

                            long offset = 0;
                            // If chunking is not supported, chunk size will be full size of the file.
                            long blockSizeDownload = !initializeFileBlocksDownloadResponse.IsChunkingSupported ? fileSizeInBytes : 4 * 1024 * 1024;

                            // File size may be smaller than defined block size
                            if (fileSizeInBytes < blockSizeDownload) { blockSizeDownload = fileSizeInBytes; }

                            while (fileSizeInBytes > 0)
                            {
                                // Prepare the request
                                DownloadBlockRequest downLoadBlockRequest = new DownloadBlockRequest
                                {
                                    BlockLength = blockSizeDownload,
                                    FileContinuationToken = fileContinuationTokenDownload,
                                    Offset = offset
                                };

                                // Send the request
                                DownloadBlockResponse downloadBlockResponse = (DownloadBlockResponse)service.Execute(downLoadBlockRequest);

                                // Add the block returned to the list
                                fileBytes.AddRange(downloadBlockResponse.Data);

                                // Subtract the amount downloaded,
                                // which may make fileSizeInBytes < 0 and indicate
                                // no further blocks to download
                                fileSizeInBytes -= (int)blockSizeDownload;
                                // Increment the offset to start at the beginning of the next block.
                                offset += blockSizeDownload;
                            }

                            // Upload File
                            InitializeFileBlocksUploadRequest initializeFileBlocksUploadRequest = new InitializeFileBlocksUploadRequest
                            {
                                Target = new EntityReference(targetTable, new Guid(targetId)),
                                FileAttributeName = targetColumn,
                                FileName = targetFilename
                            };

                            InitializeFileBlocksUploadResponse initializeFileBlocksUploadResponse = (InitializeFileBlocksUploadResponse)service.Execute(initializeFileBlocksUploadRequest);
                            string fileContinuationToken = initializeFileBlocksUploadResponse.FileContinuationToken;

                            // Capture blockids while uploading
                            List<string> blockIds = new List<string>();

                            MemoryStream uploadFileStream = new MemoryStream(fileBytes.ToArray());

                            int blockSize = 4 * 1024 * 1024; // 4 MB

                            byte[] buffer = new byte[blockSize];
                            int bytesRead = 0;
                            int blockNumber = 0;
                            // While there is unread data from the file
                            while ((bytesRead = uploadFileStream.Read(buffer, 0, buffer.Length)) > 0)
                            {
                                // The file or final block may be smaller than 4MB
                                if (bytesRead < buffer.Length) { Array.Resize(ref buffer, bytesRead); }

                                blockNumber++;
                                string blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));
                                blockIds.Add(blockId);

                                // Prepare the request
                                UploadBlockRequest uploadBlockRequest = new UploadBlockRequest
                                {
                                    BlockData = buffer,
                                    BlockId = blockId,
                                    FileContinuationToken = fileContinuationToken,
                                };

                                // Send the request
                                service.Execute(uploadBlockRequest);
                            }

                            // Commit the upload
                            CommitFileBlocksUploadRequest commitFileBlocksUploadRequest = new CommitFileBlocksUploadRequest
                            {
                                BlockList = blockIds.ToArray(),
                                FileContinuationToken = fileContinuationToken,
                                FileName = targetFilename,
                                MimeType = MimeMapping.GetMimeMapping(targetFilename)
                            };

                            CommitFileBlocksUploadResponse commitFileBlocksUploadResponse = (CommitFileBlocksUploadResponse)service.Execute(commitFileBlocksUploadRequest);
                            success = true;
                        }
                        else
                        {
                            error = $"No file is stored inside the Column {sourceColumn} of Table {sourceTable}";
                        }
                    }
                    catch (Exception ex) { error = ex.Message; }

                    context.OutputParameters["Success"] = success;
                    context.OutputParameters["Error"] = error;
                }
                catch (Exception ex)
                {
                    throw new InvalidPluginExecutionException($"CopyFileColumnToFileColumn Error. Details: {ex.Message}");
                }
            }
        }
    }
}

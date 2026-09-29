import type { Request, Response } from "express";
import crypto from "crypto";
import { sendCsvJob } from "../kafka/producer";

export async function uploadFile(req: Request, res: Response) {
  if (!req.file) {
    return res.status(400).json({
      message: "No file uploaded",
    });
  }

  const jobId = crypto.randomUUID();
  const userEmail = req.body.email;

  try {
    await sendCsvJob({
      jobId,
      fileName: req.file.originalname,
      filePath: req.file.path,
      size: req.file.size,
      userEmail
    });

    return res.status(202).json({
      message: "File uploaded and queued for processing",
      jobId,
      file: {
        originalName: req.file.originalname,
        filename: req.file.filename,
        size: req.file.size,
        path: req.file.path,
      },
    });
  } catch (error) {
    console.error("Failed to queue file in Kafka:", error);
    return res.status(500).json({
      message: "Failed to queue file for processing",
    });
  }
}
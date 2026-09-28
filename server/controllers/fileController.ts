import type { Request, Response } from "express";

export function uploadFile(req: Request, res: Response) {
  if (!req.file) {
    return res.status(400).json({
      message: "No file uploaded",
    });
  }

  console.log("File received:");
  console.log(req.file);

  return res.status(200).json({
    message: "File uploaded successfully",
    file: {
      originalName: req.file.originalname,
      filename: req.file.filename,
      size: req.file.size,
      path: req.file.path,
    },
  });
}
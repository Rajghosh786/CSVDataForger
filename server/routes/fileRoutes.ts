import { Router } from "express";
import { uploadFile, exportFile } from "../controllers/fileController";
import { upload } from "../middleware/upload";

const router = Router();

router.post("/upload", upload.single("file"), uploadFile);
router.get("/download/:token", exportFile);

export default router;
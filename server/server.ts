import express from 'express'
import cors from 'cors'
import { config } from 'dotenv'
config()
import fileRoutes from "./routes/fileRoutes"

const app = express()
const PORT = process.env.PORT || 1305;

app.use(cors({
    origin: process.env.CLIENT_URL,
    methods: ["GET", "POST", "PUT", "DELETE"],
    credentials: true
}));

app.use(express.json());

app.use('/api/files',fileRoutes)
app.listen(PORT,()=>{
    console.log(`Server started on ${PORT}`)
})

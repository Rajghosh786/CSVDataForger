import mongoose from "mongoose";
import dns from "node:dns";

dns.setServers(["1.1.1.1"]);

export const connectingDB =()=>{
    try {
        mongoose.connect(process.env.MONGO_URI)
        console.log("connected to DB")
    } catch (error) {
        console.log("error connecting DB",error)
    }
}
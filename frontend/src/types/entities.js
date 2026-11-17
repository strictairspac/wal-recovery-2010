/** @typedef {{id:number,name:string,initial:object,records:object[],description:string}} LogSet */
/** @typedef {{id:number,name:string,log_set_id:number,crash_after:number}} CrashCase */
/** @typedef {{id:number,crash_case_id:number,label:string,status:string,snapshot:object,result:object|null,attempt:number,owner:string|null,claim_reason:string|null,cancel_reason:string|null,finished_at:string|null,events?:RecoveryEvent[]}} Recovery */
/** @typedef {{id:number,recovery_id:number,event:string,actor:string,reason:string,created_at:string}} RecoveryEvent */
export {};

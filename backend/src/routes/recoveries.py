import json
from fastapi import APIRouter,Depends
from fastapi.responses import Response
from src.config.database import get_db
from src.middlewares.auth import current_user,writable
from src.schemas.recovery import RecoveryInput,CancelInput
from src.services.recovery_service import (list_recoveries,get_recovery,create_recovery,cancel_recovery,
                                           detail_payload,list_events)
from src.controllers.actions import audit
from src.utils.serialize import record
router=APIRouter()
@router.get('/recoveries')
def listing(db=Depends(get_db),user=Depends(current_user)):return list_recoveries(db)
@router.post('/recoveries',status_code=202)
def create(data:RecoveryInput,db=Depends(get_db),user=Depends(writable)):
    obj=create_recovery(db,data,actor=user.email);audit(db,user,f'recovery:enqueue:{obj.id}');db.commit();return record(obj)
@router.get('/recoveries/{ident}')
def detail(ident:int,db=Depends(get_db),user=Depends(current_user)):return detail_payload(db,get_recovery(db,ident))
@router.get('/recoveries/{ident}/events')
def events(ident:int,db=Depends(get_db),user=Depends(current_user)):
    get_recovery(db,ident);return list_events(db,ident)
@router.post('/recoveries/{ident}/cancel')
def cancel(ident:int,data:CancelInput|None=None,db=Depends(get_db),user=Depends(writable)):
    reason=data.reason if data else ''
    obj,changed=cancel_recovery(db,ident,actor=user.email,reason=reason)
    if changed:audit(db,user,f'recovery:cancel:{obj.id}')
    db.commit()
    return {**detail_payload(db,obj),'cancel_applied':changed}
@router.get('/recoveries/{ident}/export')
def export(ident:int,db=Depends(get_db),user=Depends(current_user)):
    obj=get_recovery(db,ident)
    return Response(json.dumps(detail_payload(db,obj),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="recovery-{ident}.json"'})

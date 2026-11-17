import {store} from '../stores/entities.js';
import {session} from '../stores/session.js';
import {recoveriesAPI} from '../api/entities.js';
import {table} from '../components/common/table.js';
import {badge} from '../components/common/status.js';
import {esc,formObject} from '../utils/html.js';
import {action,message} from '../hooks/action.js';
const pending=r=>r.status==='queued'||r.status==='running';
function verdict(r){if(r.status==='cancelled')return '已取消，不发布结果';if(r.result?.invariant_passed)return '与提交事务一致';if(r.result)return '需检查';return '待计算';}
function actionCell(r){if(!pending(r))return '<a href="#detail/'+r.id+'">查看恢复轨迹 →</a>';const view='<a href="#detail/'+r.id+'">查看 →</a>';if(session.role==='viewer')return view;return `${view} <button class="link danger" data-cancel="${r.id}" data-label="${esc(r.label)}">取消</button>`;}
export function runsPage(){return `<section class="split compact"><div><h2>发起一次恢复演练</h2><p>提交时冻结日志与崩溃点，后台独立重做、撤销，再与提交事务结果对照。排队或执行中的任务都可取消，确认后不再发布结果。</p></div>${session.role!=='viewer'?`<form id="run-form"><label>故障场景<select name="crash_case_id">${store.cases.map(c=>`<option value="${c.id}">${esc(c.name)}</option>`).join('')}</select></label><label>记录标签<input name="label" value="晚班恢复核对" required></label><button type="submit">冻结日志并执行 →</button></form>`:'<p class="readonly">观察员可查看下方恢复结果。</p>'}</section><div class="section-title"><h2>恢复队列</h2><button class="secondary" id="refresh">刷新状态</button></div>${table(['编号','恢复记录','状态','恢复核对','执行者','领取原因','操作'],store.runs.map(r=>[r.id,esc(r.label),badge(r.status),verdict(r),r.owner?`${esc(r.owner)} <small>#${r.attempt}</small>`:'—',esc(r.claim_reason||'—'),actionCell(r)]))}`;}
export function bindRuns(render){
 document.querySelector('#refresh').onclick=render;
 document.querySelectorAll('[data-cancel]').forEach(b=>b.onclick=async()=>{
  const id=Number(b.dataset.cancel);
  if(!confirm(`确认取消恢复演练「${b.dataset.label}」？取消后计算不再等待，结果不会发布。`))return;
  b.disabled=true;
  try{const out=await recoveriesAPI.cancel(id,'值班人员手动取消');await render();message(out.cancel_applied?'已取消，任务不再发布结果':'任务已结束，未重复取消');}
  catch(err){message(err.message,true);b.disabled=false;}
 });
 action(document.querySelector('#run-form'),async f=>{const d=formObject(f);d.crash_case_id=Number(d.crash_case_id);const job=await recoveriesAPI.create(d);location.hash='#detail/'+job.id;});
}

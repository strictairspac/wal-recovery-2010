import {table} from '../components/common/table.js';
import {badge} from '../components/common/status.js';
import {esc} from '../utils/html.js';
import {download} from '../api/client.js';
import {recoveriesAPI} from '../api/entities.js';
import {session} from '../stores/session.js';
import {RECOVERY_EVENTS} from '../constants/enums.js';
import {message} from '../hooks/action.js';
const pending=job=>job.status==='queued'||job.status==='running';
function statusPanel(job){if(job.status==='cancelled')return `<p class="notice">任务已取消${job.cancel_reason?`：${esc(job.cancel_reason)}`:'。'}结果不会发布，冻结输入与原始日志保持不变。</p>`;if(job.status==='queued')return '<p class="notice">任务排队中，点击取消可立即终止且不等待计算。</p>';if(job.status==='running')return '<p class="notice">任务执行中。取消请求立即生效，执行者随后返回的结果会被丢弃。</p>';return '';}
function evidence(job){const r=job.result;if(!r)return '<p class="notice">暂无恢复结果。</p>';return `<section class="verdict"><div><p>恢复账本核对</p><h2>${r.invariant_passed?'提交结果一致':'恢复失败'}</h2></div><p>检查点LSN ${r.checkpoint_lsn??'—'} · 崩溃点 ${r.crash_after??'—'}<br>重做 ${r.redo_count??0} 次 / 撤销 ${r.undo_count??0} 次<br>${r.error?esc(r.error):'未提交事务 '+esc((r.losers||[]).join(', ')||'无')}</p></section>${table(['账本键','检查点值','重做后','恢复后','独立提交结果'],Object.keys(r.recovered||{}).map(k=>[esc(k),r.durable_checkpoint[k],r.after_redo[k],r.recovered[k],r.oracle[k]]))}<h2>日志恢复轨迹</h2>${table(['LSN','事务','阶段','键','before → after'],(r.trace||[]).map(t=>[t.lsn,esc(t.tx),esc(t.phase),esc(t.key),t.phase==='undo_loser'||t.phase==='redo_abort_undo'?`${t.after} → ${t.before}`:`${t.before} → ${t.after}`]))}<p class="checksum">输入摘要 ${esc(r.input_sha256||'无')}<br>结果摘要 ${esc(r.result_sha256||'无')}</p>`;}
function timeline(job){const events=job.events||[];if(!events.length)return '';return `<h2>领取、接管与取消记录</h2>${table(['时间','事件','执行者','原因'],events.map(e=>[esc(e.created_at),esc(RECOVERY_EVENTS[e.event]||e.event),esc(e.actor),esc(e.reason)]))}`;}
export function detailPage(job){if(!job)return '<section class="empty"><h2>还没有恢复记录</h2><p>先选择故障场景并提交恢复任务，再逐条查看日志重做与撤销。</p><a href="#runs">进入恢复队列 →</a></section>';return `<div class="section-title"><h2>${esc(job.label)} <small>#${job.id}</small></h2>${badge(job.status)}</div><p class="meta">领取次数 ${job.attempt??0} · 当前执行者 ${job.owner?esc(job.owner):'—'} · 领取原因 ${esc(job.claim_reason||'—')}${job.finished_at?' · 结束于 '+esc(job.finished_at):''}</p>${statusPanel(job)}${evidence(job)}${timeline(job)}<div class="toolbar">${pending(job)&&session.role!=='viewer'?'<button id="cancel">取消任务</button>':''}<button id="export">导出日志与恢复证据</button><button class="secondary" id="refresh">刷新状态</button></div><details><summary>查看冻结输入</summary><pre>${esc(JSON.stringify(job.snapshot,null,2))}</pre></details>`;}
export function bindDetail(job,render){
 document.querySelector('#refresh')?.addEventListener('click',render);
 document.querySelector('#export')?.addEventListener('click',()=>download(job.id).catch(e=>message(e.message,true)));
 document.querySelector('#cancel')?.addEventListener('click',async e=>{
  if(!confirm('确认取消该恢复演练？取消后不再等待计算，结果不会发布。'))return;
  e.currentTarget.disabled=true;
  try{const out=await recoveriesAPI.cancel(job.id,'值班人员手动取消');if(location.hash!=='#detail/'+job.id)location.hash='#detail/'+job.id;await render();message(out.cancel_applied?'已取消，任务不再发布结果':'任务已结束，未重复取消');}
  catch(err){message(err.message,true);e.currentTarget.disabled=false;}
 });
}

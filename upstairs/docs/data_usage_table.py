# Manually extracted by reading the code
s = '''
pm_task(i)
    process_message
        R DownstairsIO::work
        process_ds_operation
            R   Downstairs::ds_active
            R   Downstairs::ds_state[i]
            R   Upstairs::active
            Downstairs::process_ds_completion
                R   Downstairs::ds_active
                R/W DownstairsIO::state[i]
                R/W Downstairs::io_state_count[i]
                R/W Downstairs::downstairs_errors[i]
                R/W DownstairsIO::ack_status
                R/W Downstairs::ackable_work
                W   Downstairs::ds_last_flush[i]
                R/W DownstairsIO::read_response_hashes
                R   DownstairsIO::replay
                R/W DownstairsIO::data
                R/W Downstairs::repair_info[i]
                R   DownstairsIO::work
                Downstairs::retire_check
                    R   DownstairsIO::work
                    R   DownstairsIO::state[*]
                    R   DownstairsIO::ack_status
                    R/W Downstairs::completed
                    R/W Downstairs::completed_jobs
                    R/W Downstairs::io_state_count[*]
                    R/W Downstairs::ds_active
                    R/W Downstairs::ds_skipped_jobs[*]
            Upstairs::ds_transition_with_lock(i)
                R   Downstairs::ds_state[*]
                R/W Downstairs::ds_state[i]
            Downstairs::ds_set_faulted(i)
                R   Downstairs::ds_active
                R/W DownstairsIO::state[i]
                R/W DownstairsIO::ack_status
                R/W Downstairs::ackable_work
                R/W Downstairs::io_state_count[i]
                Downstairs::retire_check
                    ...
                R/W Downstairs::ds_new[i]
                R/W Downstairs::extent_limit[i]
    R   Downstairs::ds_state[i]
    ds_deactivate
        R   Upstairs::active
        R   Downstairs::ds_active
        R   DownstairsIO::state[i]
        Upstairs::ds_transition_with_lock(i)
            ...

cmd_loop(i)
    R   Upstairs::active
    R   Downstairs::ds_state[i]
    Upstairs::ds_transition_with_lock(i)
        ...
    do_reconcile_work
        ds_transition
            Upstairs::ds_transition_with_lock(i)
                ...
        rep_in_progress
            R   Downstairs::ds_state[i]
            R/W Downstairs::reconcile_current_work
            R/W ReconcileIO::state[i]
        R   Downstairs::ds_state[i]
        R   Downstairs::reconcile_task_list
        rep_done
            R/W Downstairs::reconcile_current_work
            R/W ReconcileIO::state[i]
        ds_deactivate
            ...
    ds_transition
        ...
    Upstairs::set_inactive
        R/W Upstairs::active
    io_send
        R/W Downstairs::io_state_count[i]
        R/W Downstairs::ds_new[i]
        R/W Downstairs::flow_control[i]
        in_progress
            R/W Downstairs::ds_active
            R/W DownstairsIO::state[i]
            R/W Downstairs::io_state_count[i]
            R   DownstairsIO::work
            dependencies_need_cleanup
                R   Downstairs::ds_state[i]
                R   Downstairs::ds_skipped_jobs[i]
            remove_dep_if_live_repair
                R   Downstairs::ds_state[i]
                R   Downstairs::repair_min_id
                R   Downstairs::ds_skipped_jobs[i]

up_ds_listen
    R/W Downstairs::ackable_work
    R/W Downstairs::ds_active
    R/W DownstairsIO::ack_status
    DownstairsIO::io_size
        R   DownstairsIO::work
    R/W DownstairsIO::data
    GuestWork::gw_ds_complete
        R/W GuestWork::active
        R/W GuestWork::completed
    Downstairs::cdt_gw_work_done
        R/W Downstairs::ds_active
        R   DownstairsIO::work
    Downstairs::retire_check
        ...

up_listen
    R   Downstairs::ds_state[*]
    Upstairs::connect_region_set
        R/W Upstairs::active
        R/W Downstairs::ds_state[*]
        Upstairs::collate_downstairs
            R   Downstairs::region_metadata
            R/W Downstairs::flush_info
            R/W Downstairs::ds_state[*]
            Downstairs::convert_rc_to_messages
                R/W Downstairs::reconcile_task_list
        Upstairs::set_inactive
            ...
        R   Downstairs::ds_active
        R   Downstairs::reconcile_task_list
        Upstairs::do_reconciliation
            R/W Downstairs::reconcile_repaired
            R   Downstairs::ds_state[*]
            R/W Downstairs::reconcile_current_work
    process_new_io
        Upstairs::set_active_request
            R/W Upstairs::active
        Upstairs::set_deactivate
            R/W Upstairs::active
            R   Downstairs::ds_state[*]
            Downstairs::ds_deactivate_offline(*)
                R   Downstairs::ds_active
                R/W DownstairsIO::state[*]
                R/W Downstairs::io_state_count[*]
                R/W Downstairs::ds_new[*]
        Upstairs::submit_*
            Upstairs::guest_io_ready
                R   Upstairs::active
            Downstairs::check_repair_ids_for_range
                Downstairs::get_extent_under_repair
                    R   Downstairs::extent_limit[*]
                    R   Downstairs::repair_job_ids
                    Downstairs::reserve_repair_ids_for_extent
                        R   Downstairs::ds_active
                        R/W Downstairs::repair_job_ids
            R/W GuestWork::active
            Downstairs::enqueue
                R/W DownstairsIO::state[*]
                R   Downstairs::ds_state[*]
                R/W Downstairs::io_state_count[*]
                R/W Downstairs::ds_skipped_jobs[*]
                R/W Downstairs::ds_new[*]
                R   Downstairs::extent_limit[*]
                R/W Downstairs::ds_active
    gone_too_long
        R   Downstairs::ds_state[*]
        Downstairs::total_live_work
            R   Downstairs::io_state_count[*]
        Downstairs::ds_set_faulted(*)
            R   Downstairs::ds_active
            R/W DownstairsIO::state[*]
            R/W DownstairsIO::ack_status
            R/W Downstairs::ackable_work
            R/W Downstairs::io_state_count[*]
            Downstairs::retire_check
                ...
            R/W Downstairs::ds_new[*]
            R/W Downstairs::extent_limit[*]
        Upstairs::ds_transition_with_lock(*)
            R   Downstairs::ds_state[*]
            R/W Downstairs::ds_state[*]
    check_for_repair
        Upstairs::ds_transition_with_lock(*)
            ...
        R/W Downstairs::ro_lr_skipped
        R   Downstairs::ds_state[*]
        R/W Downstairs::repair_min_id
        Upstairs::ds_transition_with_lock(*)
            ...

live_repair_main
    R   Upstairs::active
    R/W Downstairs::repair_job_ids
    R/W Downstairs::extent_limit[*]
    R/W Downstairs::repair_min_id
    R   Downstairs::ds_state[*]
    Upstairs::abort_repair_ds
        R   Downstairs::ds_state[*]
        Downstairs::ds_set_faulted(*)
            ...
        Upstairs::ds_transition_with_lock(*)
            ...
    Downstairs::end_live_repair
        W   Downstairs::repair_info[*]
        W   Downstairs::extent_limit[*]
        W   Downstairs::repair_job_ids
        W   Downstairs::repair_min_id
    Upstairs::repair_extent
        R   Upstairs::active
        repair_ds_state_change
            R   Downstairs::ds_state[*]
        Upstairs::abort_repair_ds
            ...
        Upstairs::abort_repair_extent
            Downstairs::query_repair_ids
                R   Downstairs::repair_job_ids
            R   Downstairs::ds_state[*]
            Downstairs::end_live_repair
                ...
            Downstairs::get_repair_ids
                R/W Downstairs::repair_job_ids
                R   Downstairs::ds_active
            create_and_enqueue_noop_io
                R/W GuestWork::active
                Downstairs::enqueue_repair
                    R/W DownstairsIO::state[*]
                    R/W Downstairs::ds_active
        R/W Downstairs::extent_limit
        R/W Downstairs::repair_job_ids
        R   Downstairs::ds_active
        create_and_enqueue_reopen_io
            R/W GuestWork::active
            Downstairs::enqueue_repair
                ...
        create_and_enqueue_close_io
            R/W GuestWork::active
            Downstairs::enqueue_repair
                ...
        Upstairs::wait_and_process_repair_ack
            R   Upstairs::active
            Upstairs::abort_repair_ds
                ...
        create_and_enqueue_noop_io
            ...
        create_and_Downstairs::enqueue_repair_io
            R/W GuestWork::active
            Downstairs::enqueue_repair
                ...
    Upstairs::submit_flush
        Upstairs::submit_*
    R/W Downstairs::live_repair_aborted[*]
    R/W Downstairs::live_repair_completed[*]
    Upstairs::ds_transition_with_lock(*)
        ...
'''

stack = []
seen = {}
for line in s.split('\n'):
    if not line:
        continue
    trimmed = line.lstrip(' ')
    i = (len(line) - len(trimmed)) // 4
    stack = stack[:i] + [trimmed]
    if len(stack) > 1 and stack[-1] != '...':
        if not stack[-2] in seen:
            seen[stack[-2]] = set()
        seen[stack[-2]].add(stack[-1])

READ = 1
WRITE = 2
READ_WRITE = 3

def get_variable(k):
    if k.startswith('R '):
        return (k.lstrip('R').strip(), READ)
    elif k.startswith('W '):
        return (k.lstrip('W').strip(), WRITE)
    elif k.startswith('R/W '):
        return (k.lstrip('R/W').strip(), READ_WRITE)
    else:
        return None

variables = set()
for v in seen.values():
    for k in v:
        if k := get_variable(k):
            variables.add(k[0])

def flatten(t):
    out = {}
    for k in seen[t]:
        v = get_variable(k)
        if v:
            out[v[0]] = 0
            out[v[0]] |= v[1]
        else:
            r = flatten(k)
            for k in r:
                if not k in out:
                    out[k] = 0
                out[k] |= r[k]
    return out

# Order tasks to match the flow charg
tasks = ['up_listen', 'up_ds_listen', 'pm_task(i)', 'cmd_loop(i)', 'live_repair_main']

# Mapping of variables to tasks
flat = {t: flatten(t) for t in tasks}

# Print an asciidoc table
print(f"|")
for t in tasks:
    print(f"| `{t}`")
print()

for v in sorted(variables):
    print(f"| `{v}`")
    for t in tasks:
        if v in flat[t]:
            if flat[t][v] == READ:
                print("| R")
            elif flat[t][v] == WRITE:
                print("| W")
            elif flat[t][v] == READ_WRITE:
                print("| R/W")
            else:
                raise "Invalid R/W value"
        else:
            print("| --")
    print()

'''
for t in tasks:
    print(t)
    for (f, v) in flatten(t).items():
        if v == READ:
            print("  R   %s" % f)
        elif v == WRITE:
            print("  W   %s" % f)
        elif v == READ_WRITE:
            print("  R/W %s" % f)
        else:
            assert(false)
'''

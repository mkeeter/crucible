crucible_upstairs*:::up-guest-id
{
    gw_reserved[arg0] = timestamp;
}

crucible_upstairs*:::gw-*-start
/gw_reserved[arg0]/
{
    gw_start[arg0] = timestamp;
}

crucible_upstairs*:::up-bind-guest-id
/gw_reserved[arg0]/
{
    gw_bind[arg0] = timestamp;
    ds_to_gw[arg1] = arg0;
}

crucible_upstairs*:::up-in-progress
/ds_to_gw[arg0]/
{
    up_in_progress[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-client-message-sent
/ds_to_gw[arg0]/
{
    up_client_message_sent[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-io-sending-message
/ds_to_gw[arg0]/
{
    up_io_sending_message[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-io-message-sent
/ds_to_gw[arg0]/
{
    up_io_message_sent[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-io-message-rx
/ds_to_gw[arg0]/
{
    up_io_message_rx[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-ds-select
/ds_to_gw[arg0]/
{
    up_ds_select[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-deferring
/ds_to_gw[arg0]/
{
    up_deferring[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-defer-done
/ds_to_gw[arg0]/
{
    up_defer_done[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-client-message-received
/ds_to_gw[arg0]/
{
    up_client_message_rx[arg0, arg1] = timestamp;
}

crucible_upstairs*:::up-io-completion
/ds_to_gw[arg0]/
{
    up_io_completion[arg0, arg1] = timestamp;
}

crucible_upstairs*:::gw-*-done
/gw_reserved[arg0]/
{
    gw_done[arg0] = timestamp;
}

crucible_upstairs*:::up-guest-done
/gw_reserved[arg0]/
{
    guest_done[arg0] = timestamp;
}

crucible_upstairs*:::up-guest-done
/gw_reserved[arg0]/
{
    gw_reserved[arg0] = 0;
    gw_start[arg0] = 0;
    gw_bind[arg0] = 0;

    ds_to_gw[arg1] = 0;

    up_in_progress[arg1, 0] = 0;
    up_in_progress[arg1, 1] = 0;
    up_in_progress[arg1, 2] = 0;

    up_client_message_sent[arg1, 0] = 0;
    up_client_message_sent[arg1, 1] = 0;
    up_client_message_sent[arg1, 2] = 0;

    up_io_sending_message[arg1, 0] = 0;
    up_io_sending_message[arg1, 1] = 0;
    up_io_sending_message[arg1, 2] = 0;
    up_io_message_sent[arg1, 0] = 0;
    up_io_message_sent[arg1, 1] = 0;
    up_io_message_sent[arg1, 2] = 0;

    up_io_message_rx[arg1, 0] = 0;
    up_io_message_rx[arg1, 1] = 0;
    up_io_message_rx[arg1, 2] = 0;

    up_ds_select[arg1, 0] = 0;
    up_ds_select[arg1, 1] = 0;
    up_ds_select[arg1, 2] = 0;

    up_deferring[arg1, 0] = 0;
    up_deferring[arg1, 1] = 0;
    up_deferring[arg1, 2] = 0;

    up_defer_done[arg1, 0] = 0;
    up_defer_done[arg1, 1] = 0;
    up_defer_done[arg1, 2] = 0;

    up_client_message_rx[arg1, 0] = 0;
    up_client_message_rx[arg1, 1] = 0;
    up_client_message_rx[arg1, 2] = 0;

    up_io_completion[arg1, 0] = 0;
    up_io_completion[arg1, 1] = 0;
    up_io_completion[arg1, 2] = 0;

    gw_done[arg0] = 0;
    guest_done[arg0] = 0;
}

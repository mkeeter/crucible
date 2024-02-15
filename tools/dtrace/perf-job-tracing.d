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
    printf("ds_id: %u gw_id: %u\n", arg1, arg0);
    start = gw_reserved[arg0];
    printf("reserved -> start: %u\n", gw_start[arg0] - start);
    printf("         -> bind:  %u\n", gw_bind[arg0] - start);

    printf("client 0 tx\n");
    printf("  -> in progress:         %u\n", up_in_progress[arg1, 0] - start);
    printf("  -> client message sent: %u\n", up_client_message_sent[arg1, 0] - start);
    printf("  -> io task sending:     %u\n", up_io_sending_message[arg1, 0] - start);
    printf("  -> io task sent:        %u\n", up_io_message_sent[arg1, 0] - start);

    printf("client 1 tx\n");
    printf("  -> in progress:         %u\n", up_in_progress[arg1, 1] - start);
    printf("  -> client message sent: %u\n", up_client_message_sent[arg1, 1] - start);
    printf("  -> io task sending:     %u\n", up_io_sending_message[arg1, 1] - start);
    printf("  -> io task sent:        %u\n", up_io_message_sent[arg1, 1] - start);

    printf("client 2 tx\n");
    printf("  -> in progress:         %u\n", up_in_progress[arg1, 2] - start);
    printf("  -> client message sent: %u\n", up_client_message_sent[arg1, 2] - start);
    printf("  -> io task sending:     %u\n", up_io_sending_message[arg1, 2] - start);
    printf("  -> io task sent:        %u\n", up_io_message_sent[arg1, 2] - start);

    a = up_io_message_sent[arg1, 0] - start;
    b = up_io_message_sent[arg1, 1] - start;
    c = up_io_message_sent[arg1, 2] - start;
    io_sent_by = a > b ? (a > c ? a : c) : (b > c ? c : b);
    printf("all IO sent at %u\n", io_sent_by);
    @io_sent = quantize(io_sent_by);

    if (up_io_completion[arg1, 0]) {
        @io_reply = avg(up_io_message_rx[arg1, 0] - up_io_message_sent[arg1, 0]);
        printf("client 0 rx\n");
        printf("  -> io message rx: %u\n", up_io_message_rx[arg1, 0] - start);
        printf("  -> ds select:     %u\n", up_ds_select[arg1, 0] - start);
        printf("  -> ds deferring:  %u\n", up_deferring[arg1, 0] - start);
        printf("  -> ds defer done: %u\n", up_defer_done[arg1, 0] - start);
        printf("  -> client rx:     %u\n", up_client_message_rx[arg1, 0] - start);
        printf("  -> io completion: %u\n", up_io_completion[arg1, 0] - start);
    }

    if (up_io_completion[arg1, 1]) {
        @io_reply = avg(up_io_message_rx[arg1, 1] - up_io_message_sent[arg1, 1]);
        printf("client 1 rx\n");
        printf("  -> io message rx: %u\n", up_io_message_rx[arg1, 1] - start);
        printf("  -> ds select:     %u\n", up_ds_select[arg1, 1] - start);
        printf("  -> ds deferring:  %u\n", up_deferring[arg1, 1] - start);
        printf("  -> ds defer done: %u\n", up_defer_done[arg1, 1] - start);
        printf("  -> client rx:     %u\n", up_client_message_rx[arg1, 1] - start);
        printf("  -> io completion: %u\n", up_io_completion[arg1, 1] - start);
    }

    if (up_io_completion[arg1, 2]) {
        @io_reply = avg(up_io_message_rx[arg1, 2] - up_io_message_sent[arg1, 2]);
        printf("client 2 rx\n");
        printf("  -> io message rx: %u\n", up_io_message_rx[arg1, 2] - start);
        printf("  -> ds select:     %u\n", up_ds_select[arg1, 2] - start);
        printf("  -> ds deferring:  %u\n", up_deferring[arg1, 2] - start);
        printf("  -> ds defer done: %u\n", up_defer_done[arg1, 2] - start);
        printf("  -> client rx:     %u\n", up_client_message_rx[arg1, 2] - start);
        printf("  -> io completion: %u\n", up_io_completion[arg1, 2] - start);
    }

    printf("-> gw-done: %u\n", gw_done[arg0] - start);
    printf("-> done:    %u\n", guest_done[arg0] - start);
    @total_time = avg(guest_done[arg0] - start);

    printf("\n");

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

crucible_downstairs*:::ds-io-rx
{
    ds_io_rx[arg0] = timestamp;
}

crucible_downstairs*:::ds-io-rx-done
/ds_io_rx[arg0]/
{
    ds_io_rx_done[arg0] = timestamp;
}

crucible_downstairs*:::ds-message-rx
/ds_io_rx[arg0]/
{
    ds_message_rx[arg0] = timestamp;
}

crucible_downstairs*:::ds-work-added
/ds_io_rx[arg0]/
{
    ds_work_added[arg0] = timestamp;
}

crucible_downstairs*:::ds-new-work
/ds_io_rx[arg0]/
{
    ds_new_work[arg0] = timestamp;
}

crucible_downstairs*:::ds-work-done
/ds_io_rx[arg0]/
{
    ds_work_done[arg0] = timestamp;
}

crucible_downstairs*:::work-done
/ds_io_rx[arg0]/
{
    work_done[arg0, arg0] = timestamp;
}

crucible_downstairs*:::ds-io-tx
/ds_io_rx[arg0]/
{
    ds_io_tx[arg0] = timestamp;
}

crucible_downstairs*:::up-guest-done
/ds_io_rx[arg0]/
{
    ds_io_tx_done[arg0] = timestamp;

    start = ds_io_rx[arg0];
    printf("ds_id: %u\n", arg0);
    printf("rx done:      %u\n", ds_io_rx_done[arg0] - start);
    printf("message rx:   %u\n", ds_message_rx[arg0] - start);
    printf("work added:   %u\n", ds_work_added[arg0] - start);
    printf("new work:     %u\n", ds_new_work[arg0] - start);
    printf("ds work done: %u\n", ds_work_done[arg0] - start);
    printf("work done:    %u\n", work_done[arg0] - start);
    printf("io tx start:  %u\n", ds_io_tx[arg0] - start);
    printf("io tx done:   %u\n", ds_io_tx_done[arg0] - start);
    @total_time = avg(ds_io_tx_done[arg0] - start);

    printf("\n");

    ds_io_rx[arg0] = 0;
    ds_io_rx_done[arg0] = 0;
    ds_message_rx[arg0] = 0;
    ds_io_rx[arg0] = 0;
    ds_work_added[arg0] = 0;
    ds_new_work[arg0] = 0;
    ds_work_done[arg0] = 0;
    work_done[arg0] = 0;
    ds_io_tx[arg0] = 0;
    ds_io_tx_done[arg0] = 0;
}

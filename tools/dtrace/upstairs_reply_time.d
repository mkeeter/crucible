#pragma D option quiet

up-client-lag
{
        @ds0 = avg(arg0);
        @ds1 = avg(arg1);
        @ds2 = avg(arg2);
}

tick-1s
{
        printa("%@8d %@8d %@8d\n", @ds0, @ds1, @ds2);
        clear(@ds0);
        clear(@ds1);
        clear(@ds2);
}

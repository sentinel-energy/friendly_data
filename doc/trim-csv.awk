#!/usr/bin/env -S awk -f

BEGIN { t1 = 0; t2 = 0 }
NR == 1;
/electric_heater/ {
    if (++t1 > 2) next;
    print $0
}
/light_transport/ {
    if (++t2 > 2) next;
    if (t2 == 1) {
	print "...,...,...,...,...,..."
    }
    print $0
}

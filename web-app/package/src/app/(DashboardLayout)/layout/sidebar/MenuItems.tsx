import {
  IconCloud,
  IconCopy,
  IconDatabase,
  IconLayoutDashboard,
  IconUsersGroup,
} from "@tabler/icons-react";

import { uniqueId } from "lodash";

const Menuitems = [
  {
    navlabel: true,
    subheader: "Source",
  },
  {
    id: uniqueId(),
    title: "Sources",
    icon: IconDatabase,
    href: "/sources",
  },
  {
    id: uniqueId(),
    title: "Models",
    icon: IconCopy,
    href: "/models",
  },
  {
    navlabel: true,
    subheader: "Destination",
  },
  {
    id: uniqueId(),
    title: "Recipients",
    icon: IconUsersGroup,
    href: "/recipients",
  },
  {
    id: uniqueId(),
    title: "Destinations",
    icon: IconCloud,
    href: "/destinations",
  },
];

export default Menuitems;

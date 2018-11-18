using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Opereta.Models
{
    public class MeetingEmployee
    {
        public string EmployeeId { get; set; }
        public virtual ApplicationUser Employee { get; set; }
        public int MeetingId { get; set; }
        public virtual Meeting Meeting { get; set; }
    }
}

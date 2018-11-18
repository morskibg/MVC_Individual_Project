using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Opereta.Models
{
    public class MeetingDepartment
    {
        public int DepartmentId { get; set; }
        public virtual Department Department { get; set; }
        public int MeetingId { get; set; }
        public virtual Meeting Meeting { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Opereta.Models
{
    public class TaskEmployee
    {
        public string EmployeeId { get; set; }
        public virtual ApplicationUser Employee { get; set; }
        public int TaskId { get; set; }
        public virtual CompanyTask CompanyTask { get; set; }
    }
}

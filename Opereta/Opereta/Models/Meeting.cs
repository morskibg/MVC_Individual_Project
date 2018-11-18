using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Opereta.Models
{
    public class Meeting
    {
        public Meeting()
        {
            this.InvolvedDepartments = new HashSet<MeetingDepartment>();
            this.Participants = new HashSet<MeetingEmployee>();
        }
        public int Id { get; set; }
        public Agenda Agenda { get; set; }

        public virtual ICollection<MeetingDepartment> InvolvedDepartments { get; set; }
        public virtual ICollection<MeetingEmployee> Participants { get; set; }
    }
}
